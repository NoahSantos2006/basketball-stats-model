import pandas as pd
import json
import sys
from datetime import datetime, date, timedelta
import sqlite3
import os
sys.path.append(r"C:\Users\noahs\.vscode\basketball stats bot\main\programs")
from io import StringIO
import unicodedata


def clean_name(text):

    removed_accents_text =  "".join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != "Mn"
    )

    clean = removed_accents_text.replace(".", "")

    return clean
    
def props_parser(all_game_event_odds, curr_date, conn):
    
    translation = {

        "player_points": "PTS",
        "player_rebounds": "REB",
        "player_assists": "AST",
        "player_threes": "FG3M",
        "player_blocks": "BLK",
        "player_steals": "STL",
        "player_points_rebounds_assists": "PRA",
        "player_points_rebounds": "PTS_REB",
        "player_points_assists": "PTS_AST",
        "player_rebounds_assists": "REB_AST"

    }
    
    cursor = conn.cursor()
    print(f"Parsing props for {curr_date}...")

    cursor.execute("""

        SELECT * FROM PLAYER_PROPS 
        WHERE DATE = ?

        """, (curr_date,))
    
    fetch = cursor.fetchall()

    if fetch:

        parsed = pd.read_sql_query("SELECT * FROM PLAYER_PROPS WHERE DATE = ?", conn, params=(curr_date,))

        return parsed.to_dict(orient='records')

    if 'timestamp' in all_game_event_odds:

        is_list = all_game_event_odds['data'].apply(lambda x: isinstance(x, list))

        filtered = all_game_event_odds[is_list]['data']

        df = filtered[filtered.apply(lambda x: x != [])]

        record = []

        for row in df:

            for item in row:

                record.append(item)

        historical_df_parsed = pd.DataFrame(record)


        #matches key with drafkings so I can only get prop lines from draftkings
        draftkings_sportsbook_df = historical_df_parsed[

            (historical_df_parsed['key'] == "draftkings") |
            (historical_df_parsed['key'] == "fanduel")
            
            ].copy()
    
    else:

        player_props_series_bookmakers = all_game_event_odds['bookmakers']
        
        # from_records: turns a series of dictionaries into a dataframe
            # "Each dictionary is one row, unpack the keys into columns"
        player_props_df_bookmakers = pd.DataFrame.from_records(player_props_series_bookmakers)

        #matches key with drafkings so I can only get prop lines from draftkings
        draftkings_sportsbook_df = player_props_df_bookmakers[player_props_df_bookmakers['key'] == "draftkings"].copy()

    #each row in the column "market" is a list so I take the first index, of that list
    draftkings_sportsbook_df['markets'] = draftkings_sportsbook_df['markets'].str[0]

    # makes a dataframe from the player prop lines which is labelled as "markets" in the current data frame we have
    df = draftkings_sportsbook_df.apply(pd.Series)['markets'].apply(pd.Series)

    prop_bet = df['key'].to_list()
    outcomes = df['outcomes'].to_list()

    parser = {}
    cursor = conn.cursor()

    for i in range(len(prop_bet)):

        prop = prop_bet[i]
        player_lines = outcomes[i]

        for val in player_lines:

            player_name = val['description']
            prop_line = val['point']

            if player_name not in parser:

                parser[player_name] = {prop: prop_line}
            
            else:

                if prop not in parser[player_name]:

                    parser[player_name][prop] = prop_line
    
    for player_name, props in parser.items():
            
        curr_column = ['DATE', 'PLAYER']
        curr_values = [str(curr_date), player_name]

        for prop, line in props.items():
                
            curr_column.append(translation[prop])
            curr_values.append(line)
        
        curr_columns = ", ".join(curr_column)
        placeholders = ", ".join(['?'] * len(curr_values))

        cursor.execute(f"""

            INSERT OR REPLACE INTO PLAYER_PROPS ({curr_columns})
            VALUES({placeholders})

        """, curr_values)

        conn.commit()

    return parser

if __name__ == "__main__":

    translation = {

        "player_points": "PTS",
        "player_rebounds": "REB",
        "player_assists": "AST",
        "player_threes": "FG3M",
        "player_blocks": "BLK",
        "player_steals": "STL",
        "player_points_rebounds_assists": "PRA",
        "player_points_rebounds": "PTS_REB",
        "player_points_assists": "PTS_AST",
        "player_rebounds_assists": "REB_AST"

    }

    name_edge_cases = {
        'Carlton Carrington': 'Bub Carrington',
        'Isaiah Stewart II': 'Isaiah Stewart',
        'Nicolas Claxton': 'Nic Claxton',
        'Jimmy Butler': 'Jimmy Butler III',
        'Marvin Bagley': 'Marvin Bagley III',
        'Ronald Holland': 'Ronald Holland II',
        'DaRon Holmes': 'DaRon Holmes II',
        'Trey Jemison': 'Trey Jemison III',
        'Derrick Lively': 'Derrick Lively II',
        'Trey Murphy': 'Trey Murphy III',
        'Gary Payton': 'Gary Payton II',
        'Lindy Waters': 'Lindy Waters III',
        'Robert Williams': 'Robert Williams III',
        'Vincent Williams Jr': 'Vince Williams Jr',
        'Ron Holland': 'Ronald Holland II',
        'Herb Jones': 'Herbert Jones',
        'Derrick Jones': 'Derrick Jones Jr'
    }

    translation
    conn = sqlite3.connect(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")
    cursor = conn.cursor()

    # USE WHEN DELETING DATA FROM A TABLE
    # cursor.execute("DELETE FROM PLAYER_PROPS WHERE DATE = ?", ('2025-12-20',))
    # conn.commit()
    # sys.exit()

    curr_date_str = '2025-12-20'
    end_date_str = '2025-12-20'
    curr_date = datetime.strptime(curr_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    while curr_date <= end_date:

        odds_api_df = pd.read_sql_query("SELECT * FROM ODDS_API WHERE DATE = ?", conn, params=(str(curr_date),))
        
        if odds_api_df.empty:

            print(f"Could not find player props for {curr_date}")
            curr_date += timedelta(days=1)
            continue

        print(f"Updating the sql table player_props for {str(curr_date)}...")
        # when using pd.read_json always wrap it in a StringIO object so pandas knows its reading string data
        player_props = pd.read_json(StringIO(odds_api_df['GAME_PROPS'].iloc[0]))

        parsed = props_parser(player_props, str(curr_date), conn)

        with open('parsed.json', 'w') as f:

            json.dump(parsed, f, indent=4)
        
        exit()

        game_logs_player_names = pd.read_sql_query("SELECT * FROM player_game_logs", conn).drop_duplicates("PLAYER_ID")

        if isinstance(parsed, list):

            for player in parsed:

                player_name = player['PLAYER']

                name_cleaned = clean_name(player_name)

                if player_name in name_edge_cases:

                    name_cleaned = name_edge_cases[player_name]

                player_id_df = game_logs_player_names[game_logs_player_names['NAME_CLEAN'] == name_cleaned]

                if player_id_df.empty:

                    print(f"Could not find a player id for {name_cleaned}")
                    sys.exit(1)
                
                else:

                    player_id = int(player_id_df['PLAYER_ID'].iloc[0])

                player_name = player['PLAYER']
                    
                curr_column = ['DATE', 'PLAYER', 'PLAYER_ID']
                curr_values = [str(curr_date), player_name, player_id]

                for prop, line in player.items():
                    
                    if prop not in {"DATE", "PLAYER"}:
                        
                        curr_column.append(prop)
                        curr_values.append(line)
                
                curr_columns = ", ".join(curr_column)
                placeholders = ", ".join(['?'] * len(curr_values))

                cursor.execute(f"""

                    INSERT OR REPLACE INTO PLAYER_PROPS ({curr_columns})
                    VALUES({placeholders})

                """, curr_values)

            conn.commit()

        else:

            for player_name, props in parsed.items():
                
                name_cleaned = clean_name(player_name)

                if player_name in name_edge_cases:

                    name_cleaned = name_edge_cases[player_name]

                player_id = int(game_logs_player_names[game_logs_player_names['NAME_CLEAN'] == name_cleaned]['PLAYER_ID'].iloc[0])

                curr_column = ['DATE', 'PLAYER', "PLAYER_ID"]
                curr_values = [str(curr_date), player_name, player_id]

                for prop, line in props.items():
                    
                    curr_column.append(translation[prop])
                    curr_values.append(line)
                
                curr_columns = ", ".join(curr_column)
                placeholders = ", ".join(['?'] * len(curr_values))

                cursor.execute(f"""

                    INSERT OR REPLACE INTO PLAYER_PROPS ({curr_columns})
                    VALUES({placeholders})

                """, curr_values)

            conn.commit()   

        curr_date += timedelta(days=1)
    
    print(f"Player Props are updated.")