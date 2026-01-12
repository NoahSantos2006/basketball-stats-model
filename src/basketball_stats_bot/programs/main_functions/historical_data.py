import requests
import json
import pandas as pd
import sys
import os
from datetime import datetime, date, timedelta
import sqlite3
import time
import unicodedata
import numpy as np
import statistics
import joblib
from zoneinfo import ZoneInfo
from io import StringIO


from basketball_stats_bot.config import load_config
from nba_api.live.nba.endpoints import scoreboard, boxscore
from nba_api.stats.endpoints import leaguegamefinder, commonteamroster
from basketball_stats_bot.programs.scoring.scoring_functions import scoringv8



def clean_name(text):

    removed_accents_text =  "".join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != "Mn"
    )

    clean = removed_accents_text.replace(".", "")

    return clean
    
def get_odds_api_ids(API_KEY, date_str, conn):

    print(f"Finding odds_api game ids for {date_str}...")

    url = "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events"
    cursor = conn.cursor()

    # makes because the time set for the ids is in is 8601 format, we have to make sure the games are only showing today's
    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    day_after = str(date + timedelta(days=1))

    cursor = conn.cursor()

    check_for_existing_odds_ids = pd.read_sql_query("SELECT * FROM ODDS_API WHERE DATE = ?", conn, params=(date_str,))

    if not check_for_existing_odds_ids.empty:

        today_game_ids = check_for_existing_odds_ids['GAME_ID'].to_list()

        print(f"Already found odds-api ids for {date}")
        return today_game_ids

    

    params = {

        "apiKey": API_KEY,
        'date': f"{date_str}T05:00:00Z",
        "commenceTimeFrom": f"{date}T05:00:00Z",
        "commenceTimeTo": f"{day_after}T04:59:59Z"

    }

    response = requests.get(url, params=params)
    data = response.json()

    print(f"Retrieving Odds-api game ids for {curr_date}...")

    today_game_ids = []

    for val in data['data']:
        
        today_game_ids.append(val['id'])

        cursor.execute("""

            INSERT OR REPLACE INTO ODDS_API (DATE, GAME_ID)
            VALUES (?, ?)
            
        """, (str(curr_date), val['id']))
    
    conn.commit()

    return today_game_ids

def get_historical_prop_lines(date, today_ids, apiKey, conn):

    player_props = [
        "player_points",
        "player_rebounds",
        "player_assists",
        "player_threes",
        "player_points_rebounds_assists",
        "player_points_rebounds",
        "player_points_assists",
        "player_rebounds_assists",
    ]

    dfs = []
    cursor = conn.cursor()
    check = pd.read_sql_query("SELECT * FROM ODDS_API WHERE DATE = ?", conn, params=(date,))

    if not check.empty:
        
        if not check['GAME_PROPS'].iloc[0] == None:

            print(f"Already found odds-api game props json file for {date}")
            return check['GAME_PROPS'].iloc[0]
        
    print(f"Finding props for {date}...\n")

    for gameID in today_ids:
        
        url = f"https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events/{gameID}/odds"

        for prop in player_props:
            
            temp = datetime.strptime(date, "%Y-%m-%d").date()
            day_after = temp + timedelta(days=1)

            params = {

                "apiKey": apiKey,
                "date": f"{date}T09:00:00Z",
                "regions": "us",
                "markets": prop,
                "odds_format": "american",
                "commenceTimeTo": f"{str(day_after)}T04:59:59Z"

            }

            print(f"Retrieving {prop} from {gameID}...")

            response = requests.get(url, params=params)

            data = response.json()

            if 'message' in data:

                print(f"{data['message']}")

                game_props_df = pd.concat(dfs, ignore_index=True)

                file_path = os.path.join(config.GAMES_FILE_PATH, str(date), "player_props_corrupted.json")

                game_props_df.to_json(file_path, orient="records", indent=4)

                cursor.execute("""

                    INSERT OR REPLACE INTO ODDS_API (DATE, GAME_ID, GAME_PROPS)
                    VALUES (?, ?, ?)

                """, (str(date), gameID, game_props_df.to_json()))
                
                conn.commit()
                
                print(f"{data['message']}")

                print(f"Due to the following error, the game props json file for {date} might be corrupted..")

                return game_props_df

            df = pd.DataFrame(data)

            dfs.append(df)

    game_props_df = pd.concat(dfs, ignore_index=True)
    
    for game_id in today_ids:

        cursor.execute("""

                INSERT OR REPLACE INTO ODDS_API (DATE, GAME_ID, GAME_PROPS)
                VALUES (?, ?, ?)

            """, (str(date), game_id, game_props_df.to_json()))

    file_path = os.path.join(config.GAME_FILES_PATH, str(date), "player_props.json")

    game_props_df.to_json(file_path, orient="records", indent=4)

    return game_props_df

def props_parser(all_game_event_odds, conn, current_date):
    
    translation = {

        "player_points": "PTS",
        "player_rebounds": "REB",
        "player_assists": "AST",
        "player_threes": "FG3M",
        'player_steals': 'STL',
        'player_blocks': 'BLK',
        "player_points_rebounds_assists": "PRA",
        "player_points_rebounds": "PTS_REB",
        "player_points_assists": "PTS_AST",
        "player_rebounds_assists": "REB_AST"

    }

    reverse_translation = {
        "PTS": "player_points",
        "REB": "player_rebounds",
        "AST": "player_assists",
        "FG3M": "player_threes",
        'STL': 'player_steals',
        'BLK': 'player_blocks',
        "PRA": "player_points_rebounds_assists",
        "PTS_REB": "player_points_rebounds",
        "PTS_AST": "player_points_assists",
        "REB_AST": "player_rebounds_assists"
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
        'Derrick Jones': 'Derrick Jones Jr',
        'Paul Reed Jr': 'Paul Reed'
    }

    cursor = conn.cursor()
    print(f"Parsing props for {current_date}...")

    cursor.execute("""

        SELECT * FROM PLAYER_PROPS 
        WHERE DATE = ?

        """, (current_date,))
    
    fetch = cursor.fetchall()

    if fetch:

        parsed = pd.read_sql_query("SELECT * FROM PLAYER_PROPS WHERE DATE = ?", conn, params=(current_date,)).to_dict(orient='records')

        parser = {}

        for hashmap in parsed:
            
            player_name = hashmap['PLAYER']
            parser[player_name] = {}

            for prop, line in hashmap.items():

                if prop not in {'DATE', 'PLAYER', 'PLAYER_ID'} and not pd.isna(line) and line != None:

                    parser[player_name][reverse_translation[prop]] = line
        
        return parser

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
    
    # converts the list of dictionaries into one big dictionary with key = player_name and values = props
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
    
    print("Updating PLAYER PROPS table")

    game_logs_player_names = pd.read_sql_query("SELECT * FROM player_game_logs", conn).drop_duplicates("PLAYER_ID")

    for player_name, props in parser.items():

        name_cleaned = clean_name(player_name)

        if player_name in name_edge_cases:

            name_cleaned = name_edge_cases[player_name]

        player_id = int(game_logs_player_names[game_logs_player_names['NAME_CLEAN'] == name_cleaned]['PLAYER_ID'].iloc[0])
        
        curr_column = ['DATE', 'PLAYER', 'PLAYER_ID']
        curr_values = [current_date, player_name, player_id]

        for prop, line in props.items():
            
            print(f"Adding {prop} for {player_name}...")
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

def scoreboard_to_team_roster(current_season, curr_date, conn):

    def get_roster(home_team_id, teamTricode, opposition_id, opposition_tricode, current_season, venue, curr_game_id):

        roster = commonteamroster.CommonTeamRoster(
            team_id=home_team_id,
            season=current_season,
            timeout=30
        )

        df = roster.get_data_frames()[0]

        if venue == "home":

            df['MATCHUP'] = f"{teamTricode} vs. {opposition_tricode}"
        
        else:

            df['MATCHUP'] = f"{teamTricode} @ {opposition_tricode}"

        df['GAME_ID'] = curr_game_id
        df['date'] = curr_date
        df['team_tricode'] = teamTricode
        df['opposition_team_id'] = opposition_id
        df['opposition_tricode'] = opposition_tricode
        
        time.sleep(0.5)

        return df
    
    today = date.today()

    if curr_date == str(today):

        board = scoreboard.ScoreBoard()

        if not board:

            print(f"No games on {curr_date}.")
            exit()

        games = board.games.get_dict()

        games_dict = {}
        gameIds = [game['gameId'] for game in games]

        for game in games:

            games_dict[game['gameId']] = {

                'homeTeam': {
                    'teamName': f"{game['homeTeam']['teamCity']} {game['homeTeam']['teamName']}",
                    'teamId': game['homeTeam']['teamId'],
                    'teamTricode': game['homeTeam']['teamTricode'],
                },

                'awayTeam': {
                    'teamName': f"{game['awayTeam']['teamCity']} {game['awayTeam']['teamName']}",
                    'teamId': game['awayTeam']['teamId'],
                    'teamTricode': game['awayTeam']['teamTricode'],
                }
            }

    else:

        gamefinder = leaguegamefinder.LeagueGameFinder(
                    date_from_nullable=curr_date,
                    date_to_nullable=curr_date
                )

        games_df = gamefinder.get_data_frames()[0]
        gameIds = list(games_df['GAME_ID'].drop_duplicates())

        team_tricode_arr = games_df['TEAM_ABBREVIATION'].to_list()
        team_id_arr = games_df['TEAM_ID'].to_list()
        matchup_arr = games_df['MATCHUP'].to_list()
        game_id_arr = games_df['GAME_ID'].to_list()
        team_name_arr = games_df['TEAM_NAME'].to_list()

        games_dict = {}

        for i in range(len(game_id_arr)):

            if game_id_arr[i] in games_dict:

                curr = games_dict[game_id_arr[i]]

                if not curr['homeTeam']['teamId']:
                    
                    curr['homeTeam']['teamName'] = team_name_arr[i]
                    curr['homeTeam']['teamId'] = team_id_arr[i]
                    curr['homeTeam']['teamTricode'] = team_tricode_arr[i]
                
                else:
                    
                    curr['awayTeam']['teamName'] = team_name_arr[i]
                    curr['awayTeam']['teamId'] = team_id_arr[i]
                    curr['awayTeam']['teamTricode'] = team_tricode_arr[i]
                
            else:

                games_dict[game_id_arr[i]] = {

                    'homeTeam': {
                        'teamName': None,
                        'teamId': None,
                        'teamTricode': None
                    }, 
                    'awayTeam': {
                        'teamName': None,
                        'teamId': None,
                        'teamTricode': None
                    }

                }

                curr = games_dict[game_id_arr[i]]

                if '@' in matchup_arr[i]:

                    curr['awayTeam']['teamName'] = team_name_arr[i]
                    curr['awayTeam']['teamId'] = team_id_arr[i]
                    curr['awayTeam']['teamTricode'] = team_tricode_arr[i]
                
                else:

                    curr['homeTeam']['teamName'] = team_name_arr[i]
                    curr['homeTeam']['teamId'] = team_id_arr[i]
                    curr['homeTeam']['teamTricode'] = team_tricode_arr[i]

    today_nba_api_gameIds = []

    cursor = conn.cursor()

    for game in gameIds:
        
        today_nba_api_gameIds.append(game)
        cursor.execute("INSERT OR REPLACE INTO NBA_API_GAME_IDS (DATE, GAME_ID) VALUES (?, ?)", (str(curr_date), game))
    
    conn.commit()

    dir_path = os.path.join(config.GAME_FILES_PATH, str(curr_date))

    if not os.path.isdir(dir_path):

        os.mkdir(dir_path)

    file_path = os.path.join(config.GAME_FILES_PATH, str(curr_date), "nba_api_game_ids.json")

    with open(file_path, "w") as f:

        json.dump(today_nba_api_gameIds, f, indent=4)

    dfs = []

    check_for_existing_df = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(str(curr_date),))

    if not check_for_existing_df.empty:

        print(f"Already found a scoreboard to roster dataframe for {curr_date}")

        return check_for_existing_df

    print(f"\nFinding rosters for {curr_date}...\n")
    
    for curr_game_id, game in games_dict.items():

        home_team = game['homeTeam']
        away_team = game['awayTeam']

        print(f"Extracting roster from the {home_team['teamName']}\n")
        dfs.append(get_roster(home_team['teamId'], home_team['teamTricode'], away_team['teamId'], away_team['teamTricode'], current_season, "home", curr_game_id))

        print(f"Extracting roster from the {away_team['teamName']}\n")
        dfs.append(get_roster(away_team['teamId'], away_team['teamTricode'], home_team['teamId'], home_team['teamTricode'], current_season, "away", curr_game_id))
    
    scoreboard_to_team_roster_df = pd.concat(dfs, ignore_index=True)

    file_path = os.path.join(config.GAME_FILES_PATH, str(curr_date), "scoreboard_to_team_roster_output.json")

    scoreboard_to_team_roster_df.to_json(file_path, orient="records", indent=4)

    cursor = conn.cursor()

    row = list(scoreboard_to_team_roster_df)
    row_names = ", ".join(row)
    placeholders = ", ".join(['?'] * len(row))

    for _, row in scoreboard_to_team_roster_df.iterrows():

        cursor.execute(f"""

            INSERT OR REPLACE INTO SCOREBOARD_TO_ROSTER ({row_names})
            VALUES ({placeholders})

        """, row.to_list())

        conn.commit()

    return scoreboard_to_team_roster_df

def player_vs_team_or_last_20(scoreboard_to_team_roster_df, date, current_season_start_date, conn):

    config = load_config()
    
    def find_player_game_logs_df(all_player_game_logs, player_name, player_id, opposition_team_id, curr_date):

            print(f"Finding gamelogs for {player_name}...")

            player_vs_team_df = all_player_game_logs[
                (all_player_game_logs['PLAYER_ID'] == player_id) &
                (all_player_game_logs['OPPOSITION_ID'] == opposition_team_id) &
                (all_player_game_logs['GAME_DATE'] < curr_date) &
                (all_player_game_logs['MIN'] > 0)
            ]

            last_20_games_df = all_player_game_logs[
                (all_player_game_logs['PLAYER_ID'] == player_id) &
                (all_player_game_logs['OPPOSITION_ID'] != opposition_team_id) &
                (all_player_game_logs['GAME_DATE'] >= current_season_start_date) &
                (all_player_game_logs['GAME_DATE'] < curr_date) &
                (all_player_game_logs['MIN'] > 0)
            ]

            player_vs_team_df = player_vs_team_df = player_vs_team_df.sort_values("GAME_DATE", ascending=False).iloc[:10]
            last_20_games_df = last_20_games_df.sort_values("GAME_DATE", ascending=False).iloc[:20]

            for cell in ['FT_PCT', 'FG_PCT', 'FG3_PCT']:
                 
                 player_vs_team_df[cell] = player_vs_team_df[cell].astype('float64')
                 last_20_games_df[cell] = last_20_games_df[cell].astype('float64')

            player_vs_team_and_last_20_df = pd.concat([player_vs_team_df, last_20_games_df], ignore_index=True)

            player_vs_team_and_last_20_df = player_vs_team_and_last_20_df.sort_values('GAME_DATE', ascending=False).reset_index(drop=True)

            return player_vs_team_and_last_20_df

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM PLAYER_VS_TEAM_OR_LAST_20_JSONS WHERE DATE = ?", (str(date),))
    fetch = cursor.fetchall()

    if fetch:
        
        df = pd.read_json(StringIO(fetch[0][1]))

        return df

    print(f"\nFinding player game logs for {date}\n")

    player_names = scoreboard_to_team_roster_df['PLAYER'].to_list()
    player_ids = scoreboard_to_team_roster_df["PLAYER_ID"].tolist()
    opposition_team_ids = scoreboard_to_team_roster_df["opposition_team_id"].tolist()
    all_player_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs", conn)

    dfs = []

    for player_name, player_id, opp_id in list(zip(player_names, player_ids, opposition_team_ids)):
        
        curr_df = find_player_game_logs_df(all_player_game_logs, player_name, player_id, opp_id, str(date))

        if not curr_df.empty:
            dfs.append(curr_df)
        
    player_vs_team_or_last_20_df = pd.concat(dfs, ignore_index=True)

    player_vs_team_or_last_20_path = os.path.join(config.GAME_FILES_PATH, str(date), 'player_vs_team_or_last_20.json')

    player_vs_team_or_last_20_df.to_json(player_vs_team_or_last_20_path, orient='records', indent=4)

    cursor.execute("INSERT OR REPLACE INTO PLAYER_VS_TEAM_OR_LAST_20_JSONS VALUES(?, ?)", (str(date), player_vs_team_or_last_20_df.to_json()))
    conn.commit()

    return player_vs_team_or_last_20_df

def player_vs_prop_scores(player_vs_team_or_last_20_df, draftkings_sportsbook, date, conn, season_start_date):

    config = load_config()
    
    def find_minutes_projection(season_game_logs, curr_scoreboard, positions_df, season_start_date, curr_date, player_id, conn):

        def find_minute_projection_features(conn, season_start_date, curr_date, season_game_logs, curr_scoreboard, positions_df, player_id):

            def avg_last_3_5_7_10(game_logs, player_id, curr_date):

                game_logs = game_logs[
                    (game_logs['GAME_DATE'] < str(curr_date)) &
                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['MIN'] > 0)
                ]

                minutes_list = game_logs['MIN'].to_list()
                last_3 = []
                last_5 = []
                last_7 = []
                last_10 = []

                if len(minutes_list) == 0:

                    return np.nan, np.nan, np.nan, np.nan

                i = 0

                while i < min(10, len(minutes_list)):

                    if len(last_3) < 3:
                        last_3.append(minutes_list[i])       
                    if len(last_5) < 5:
                        last_5.append(minutes_list[i])
                    if len(last_7) < 7:
                        last_7.append(minutes_list[i])
                    if len(last_10) < 10:
                        last_10.append(minutes_list[i])

                    i += 1
                
                curr_average = sum(minutes_list) / len(minutes_list)

                while len(last_10) < 10:

                    if len(last_3) < 3:
                        last_3.append(curr_average)       
                    if len(last_5) < 5:
                        last_5.append(curr_average)
                    if len(last_7) < 7:
                        last_7.append(curr_average)
                    if len(last_10) < 10:
                        last_10.append(curr_average)

                average_last_3 = float(sum(last_3) / 3)
                average_last_5 = float(sum(last_5) / 5)
                average_last_7 = float(sum(last_7) / 7)
                average_last_10 = float(sum(last_10) / 10)

                return average_last_3, average_last_5, average_last_7, average_last_10

            def minutes_trend_5(curr_date, player_id, player_name, season_game_logs):
                
                curr_player_game_logs = season_game_logs[
                    (season_game_logs['GAME_DATE'] < str(curr_date)) &
                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['MIN'] > 0)
                ]
                
                minutes_list = curr_player_game_logs['MIN'].to_list()

                last_5 = []
                average = 0

                if len(minutes_list) == 0:

                    print(f"Could not find games with minutes before {curr_date} for {player_name}.")
                    return np.nan
                
                last_5 = minutes_list[:5]

                if len(last_5) < 2:

                    return np.nan
                
                average = sum(last_5) / len(last_5)

                while len(last_5) < 5:

                    last_5.append(average)

                slope = (last_5[-1] - last_5[0]) / 4

                return slope

            def find_position_missing_minutes(conn, curr_date, positions, team_id):

                dfs = []

                for position in positions:

                    df = pd.read_sql_query("""

                            SELECT d.*
                            FROM DNPS d
                            JOIN PLAYER_POSITIONS p
                                ON p.PLAYER_ID = d.PLAYER_ID
                            WHERE p.POSITION = ?
                            AND d.GAME_DATE = ?
                            AND d.TEAM_ID = ?
                                        
                        """, conn, params=(position, str(curr_date), team_id))

                    if not df.empty:
                        dfs.append(df)
                
                if not dfs:

                    return 0

                cat = pd.concat(dfs, ignore_index=True)

                total_pos_minutes = cat.drop_duplicates('PLAYER_ID')['AVERAGE_MINUTES'].sum()
                
                return min(48, total_pos_minutes)

            def find_last_10_std_dev(curr_date, player_id, player_name, game_logs):

                current_player_game_logs = game_logs[
                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['GAME_DATE'] < curr_date) &
                    (game_logs['MIN'] > 0)
                ]

                if current_player_game_logs.empty:

                    print(f"Could not find game logs before {curr_date} for {player_name}")
                    return np.nan

                last_10_games = current_player_game_logs.iloc[:10]['MIN'].to_list()

                if len(last_10_games) < 5:

                    return np.nan

                std_dev = statistics.stdev(last_10_games)

                return std_dev

            def find_days_rest(curr_date, player_id, game_logs, season_start_date_str):

                curr_player_game_logs = game_logs[

                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['GAME_DATE'] < curr_date) &
                    (game_logs['MIN'] > 0)
                ]

                last_game_date = datetime.strptime(curr_date, "%Y-%m-%d").date() - timedelta(days=1)
                season_start_date = datetime.strptime(season_start_date_str, "%Y-%m-%d").date()

                days_rest = 0

                while last_game_date >= season_start_date:

                    if not curr_player_game_logs[curr_player_game_logs['GAME_DATE'] == str(last_game_date)].empty:

                        return days_rest

                    days_rest += 1
                    last_game_date -= timedelta(days=1)
                
                return np.nan

            def find_total_games_played_this_season(curr_date, player_id, game_logs):

                current_player_season_game_logs = len(
                    game_logs[

                        (game_logs['PLAYER_ID'] == player_id) &
                        (game_logs['GAME_DATE'] < curr_date) &
                        (game_logs['MIN'] > 0)
                    ]
                )

                return current_player_season_game_logs

            def find_if_back_to_back(curr_date, player_id, game_logs):

                day_before_curr_date = datetime.strptime(curr_date, "%Y-%m-%d").date() - timedelta(days=1)

                if game_logs[

                    (game_logs['GAME_DATE'] == str(day_before_curr_date)) &
                    (game_logs['PLAYER_ID'] == player_id)

                ].empty:
                    
                    return 0

                return 1

            def find_games_started_last_5(curr_date, player_id, season_game_logs):

                player_game_logs = season_game_logs[
                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['GAME_DATE'] < curr_date) &
                    (season_game_logs['MIN'] > 0)
                ].iloc[:5]

                if player_game_logs.empty:

                    return np.nan

                return len(player_game_logs[
                    (player_game_logs['STARTER'] == 1)
                ])

            def find_games_played_last_5_10_compared_to_team(curr_date, player_id, season_game_logs):

                player_logs = season_game_logs[

                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['GAME_DATE'] < str(curr_date)) 
                    
                ].iloc[:10]

                if player_logs.empty:

                    return np.nan, np.nan

                team_id = player_logs['TEAM_ID'].iloc[0]

                team_logs = season_game_logs[

                    (season_game_logs['TEAM_ID'] == team_id) &
                    (season_game_logs['GAME_DATE'] < str(curr_date))

                ].drop_duplicates("GAME_DATE").iloc[:10]

                team_logs = team_logs.sort_values("GAME_DATE", ascending=False)

                games_played_last_5 = 0
                games_played_last_10 = 0
                team_logs_game_id_list = team_logs['GAME_ID'].to_list()
                player_logs_game_id_list = player_logs['GAME_ID'].to_list()

                i = 0

                for gameId in team_logs_game_id_list:
                    
                    if gameId in player_logs_game_id_list:

                        if games_played_last_5 < 5:

                            games_played_last_5 += 1
                        
                        games_played_last_10 += 1
                        
                    i += 1

                    if i == 10:

                        break
                
                return games_played_last_5, games_played_last_10

            positions = positions_df[positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()
            team_id = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['TeamID'].iloc[0]
            player_name = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]

            average_last_3, average_last_5, average_last_7, average_last_10 = avg_last_3_5_7_10(season_game_logs, player_id, str(curr_date))
            minute_trend = minutes_trend_5(str(curr_date), player_id, player_name, season_game_logs)
            position_missing_minutes = find_position_missing_minutes(conn, str(curr_date), positions, int(team_id))
            last_10_std_dev = find_last_10_std_dev(str(curr_date), player_id, player_name, season_game_logs)
            days_rest = find_days_rest(str(curr_date), player_id, season_game_logs, season_start_date)
            total_games_played_this_season = find_total_games_played_this_season(str(curr_date), player_id, season_game_logs)
            is_back_to_back = find_if_back_to_back(str(curr_date), player_id, season_game_logs)
            games_started_last_5 = find_games_started_last_5(str(curr_date), player_id, season_game_logs)
            games_played_last_5, games_played_last_10 = find_games_played_last_5_10_compared_to_team(str(curr_date), player_id, season_game_logs)

            features_dict = {
                "AVERAGE_LAST_3": average_last_3, 
                "AVERAGE_LAST_5": average_last_5, 
                "AVERAGE_LAST_7": average_last_7, 
                "AVERAGE_LAST_10": average_last_10, 
                "MINUTE_TREND": minute_trend, 
                "POSITION_MISSING_MINUTES": position_missing_minutes, 
                "LAST_10_STANDARD_DEVIATION": last_10_std_dev,
                "DAYS_OF_REST": days_rest,
                "TOTAL_GAMES_PLAYED_THIS_SEASON": total_games_played_this_season,
                "IS_BACK_TO_BACK": is_back_to_back,
                "GAMES_STARTED_LAST_5": games_started_last_5,
                "GAMES_PLAYED_LAST_5": games_played_last_5,
                "GAMES_PLAYED_LAST_10": games_played_last_10,
            }


            return features_dict

        minutes_projection_features = pd.DataFrame([find_minute_projection_features(
            conn=conn, 
            season_start_date=season_start_date, 
            curr_date=curr_date,
            season_game_logs=season_game_logs,
            curr_scoreboard=curr_scoreboard,
            positions_df=positions_df,
            player_id = player_id
        )])

        minutes_projection_model_path = os.path.join(config.XGBOOST_PATH, "minutes_projection_model.pkl")

        model = joblib.load(minutes_projection_model_path)

        minutes_projection = model.predict(minutes_projection_features)[0]

        return minutes_projection

    def clean_name(text):

        removed_accents_text =  "".join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != "Mn"
        )

        clean = removed_accents_text.replace(".", "")

        return clean
    
    translation = {

        "player_points": "PTS",
        "player_rebounds": "REB",
        "player_assists": "AST",
        "player_threes": "FG3M",
        "player_blocks": "BLK",
        "player_steals": "STL",
        "player_blocks_steals": "BLK_STL",
        "player_turnovers": "TOV",
        "player_points_rebounds_assists": "PRA",
        "player_points_rebounds": "PTS_REB",
        "player_points_assists": "PTS_AST",
        "player_rebounds_assists": "REB_AST",
        "player_field_goals": "FGM",
        "player_frees_made": "FTM",
        "player_frees_attempt": "FGA"

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
        'Derrick Jones': 'Derrick Jones Jr',
        'Paul Reed Jr': 'Paul Reed'
    }

    system = {}
    conn.create_function("clean_name", 1, clean_name)

    scoreboard = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE DATE = ?", conn, params=(date,))
    scoreboard['PLAYER'] = scoreboard['PLAYER'].apply(clean_name)
    player_positions_df = pd.read_sql_query("SELECT * FROM PLAYER_POSITIONS", conn)
    team_totals_df = pd.read_sql_query("SELECT * FROM TEAM_STATS_2025_2026", conn)
    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE < ? AND GAME_DATE >= ? ORDER BY GAME_DATE DESC", conn, params=(date, season_start_date))

    if 'name_clean' not in player_vs_team_or_last_20_df.columns:

        player_games = {
            player: df for player, df in player_vs_team_or_last_20_df.groupby("NAME_CLEAN")
        }

    else:

        player_games = {
            player: df for player, df in player_vs_team_or_last_20_df.groupby("name_clean")
        }

    system = {}

    for player, prop_lines in draftkings_sportsbook.items():

        print(f"Calculating score for {player} using scoringv8")

        player = clean_name(player)

        if player in name_edge_cases:

            player = name_edge_cases[player]

        if player not in system:

            system[player] = {}

        if clean_name(player) not in player_games:

            print(f"Could not find {player} in player_games. Check historical_data.py: Line 1087")
            continue
        
        curr_player_vs_team_or_last_20_df = player_games[player]
        curr_player_vs_team_or_last_20_df = curr_player_vs_team_or_last_20_df.sort_values('GAME_DATE', ascending=False)

        if curr_player_vs_team_or_last_20_df.empty:

            continue
            
        player_id = curr_player_vs_team_or_last_20_df['PLAYER_ID'].iloc[0]
        current_opposition_ID = scoreboard[scoreboard["PLAYER_ID"] == player_id]['opposition_team_id'].iloc[0]

        minutes_projection = find_minutes_projection(season_game_logs=season_game_logs, 
                                                        curr_scoreboard=scoreboard, 
                                                        positions_df=player_positions_df, 
                                                        season_start_date=season_start_date, 
                                                        curr_date=date, 
                                                        player_id=player_id, 
                                                        conn=conn
                                                    )

        for prop, line in prop_lines.items():
                            
            curr_score = scoringv8(curr_player_vs_team_or_last_20_df, current_opposition_ID, translation[prop], line, scoreboard, player_positions_df, date, team_totals_df, minutes_projection, season_game_logs)

            if not curr_score or curr_score == -2:

                continue
            
            
            system[player][prop] = (float(curr_score), line)
            system[player]['PERSON_ID'] = int(player_vs_team_or_last_20_df[player_vs_team_or_last_20_df['NAME_CLEAN'] == player]['PLAYER_ID'].iloc[0])
    
    file_path = os.path.join(config.GAME_FILES_PATH, str(date), "scores.json")

    with open(file_path, "w") as f:

        json.dump(system, f, indent=4)

    return system

def result(scores, date, conn):
        
    class Node:

        def __init__(self, val):

            self.val = val
            self.next = None

    class LinkedList():

        def __init__(self):

            self.head = None
        
        def appendNode(self, node_val):

            node = Node(node_val)

            prev = None
            curr = self.head

            while curr:
                
                prev = curr
                curr = curr.next
            
            if not prev:

                self.head = node
            
            else:

                prev.next = node

        def insert_reverse_sorted(self, player_name, prop, score, line, matchup, curr_date, player_id):

            if score < 0:

                over_under = "U"
            
            elif score > 0:

                over_under = "O"
            
            else:

                over_under = "O/U"
            
            now = time.strftime(f"%Y-%m-%d %H:%M:%S")

            node = Node({
                'DATE': curr_date,
                "PLAYER": player_name, 
                "OVER_UNDER": over_under, 
                "PROP": prop, 
                "LINE": line, 
                "MATCHUP": matchup,
                "SCORE": abs(score),
                "LAST_UPDATED": now,
                "PLAYER_ID": player_id
                })

            prev = None
            curr = self.head

            while curr and (curr.val['SCORE'] > node.val['SCORE']):

                prev = curr
                curr = curr.next
            
            if not prev:
                
                node.next = self.head
                self.head = node

            else:

                prev.next = node
                node.next = curr

        def to_array(self):

            arr = []

            if not self.head:

                return arr

            curr = self.head

            while curr:

                arr.append(curr.val)
                curr = curr.next
            
            return arr

        def __repr__(self):
            
            if not self.head:

                return ""
            
            string = []

            string.append("[")

            curr = self.head

            while curr:

                string.append(f"{curr.val}")

                if not curr.next:

                    string.append("]")
                
                else:

                    string.append(", ")

                curr = curr.next
            
            return "".join(string)
        
    system = LinkedList()

    for player_name, score_dict in scores.items():
        
        for prop, score in score_dict.items():

            if prop == 'PERSON_ID':
                continue

            currScore, line = score

            rosters = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(str(date),))
            
            curr = rosters[rosters['PLAYER_ID'] == score_dict["PERSON_ID"]]

            matchup = curr['MATCHUP'].to_list()[0]

            system.insert_reverse_sorted(player_name, prop, currScore, line, matchup, date, score_dict["PERSON_ID"])

    system_sorted = system.to_array()

    file_path = os.path.join(config.GAME_FILES_PATH, str(date), "system.json")

    with open(file_path, "w") as f:

        json.dump(system_sorted, f, indent=4)
    
    cursor = conn.cursor()

    for node in system_sorted:

        placeholders = ", ".join(['?'] * len(node))

        cursor.execute(f"""

            INSERT OR REPLACE INTO SYSTEM (DATE, PLAYER, OVER_UNDER, PROP, LINE, MATCHUP, SCORE, LAST_UPDATED, PLAYER_ID)
            VALUES ({placeholders})

        """, list(node.values()))
    
    conn.commit()
    print(f"\nDatabase table SYSTEM updated.\n")

    print(f"System was added in: {file_path}\n")

    return system_sorted

def system_grade(date, conn):
    
    cursor = conn.cursor()

    today_nba_api_game_ids = pd.read_sql_query("SELECT * FROM NBA_API_GAME_IDS WHERE DATE = ?", conn, params=(str(date),))

    if today_nba_api_game_ids.empty:

        print(f"No games found for {date}")
        return pd.DataFrame()
    
    else:

        today_nba_api_game_ids = today_nba_api_game_ids['GAME_ID'].to_list()

    allteamboxscores = []

    for gameId in today_nba_api_game_ids:

        print(f"Finding the boxscore for {gameId}...")
        
        box = boxscore.BoxScore(gameId)

        stats = box.get_dict()['game']

        allteamboxscores.append(pd.DataFrame(stats['homeTeam']['players']))
        allteamboxscores.append(pd.DataFrame(stats['awayTeam']['players']))


    today_box_scores = pd.concat(allteamboxscores, ignore_index=True)

    player_ids = today_box_scores['personId'].to_list()

    system = pd.read_sql_query("SELECT * FROM SYSTEM WHERE DATE = ? ORDER BY SCORE DESC", conn, params=(str(date),))

    system["RESULT"] = pd.NA

    translation = {
        "player_points": 'points',
        "player_rebounds": 'reboundsTotal',
        "player_assists": "assists",
        "player_threes": "threePointersMade",
        "player_blocks": "blocks",
        "player_steals": "steals",
    }

    for pid in player_ids:

        curr_player_boxscore = today_box_scores[today_box_scores['personId'] == pid]
        curr_player_status = curr_player_boxscore['status'].iloc[0]

        if curr_player_status == 'INACTIVE':

            cursor.execute("""

                DELETE FROM SYSTEM
                WHERE DATE = ?
                AND PLAYER_ID = ?

            """, (str(date), pid))

            continue
        
        currStats = curr_player_boxscore['statistics'].iloc[0]
        currLines = system[system['PLAYER_ID'] == pid]
        props = currLines['PROP'].to_list()

        for prop in props:
            
            if prop == "player_points_rebounds_assists":

                curr_line = currStats['points'] + currStats['reboundsTotal'] + currStats['assists']

            elif prop == "player_points_rebounds":

                curr_line = currStats['points'] + currStats['reboundsTotal']

            elif prop == "player_points_assists":

                curr_line = currStats['points'] + currStats['assists']

            elif prop == "player_rebounds_assists":

                curr_line = currStats['reboundsTotal'] + currStats['assists']
            
            else:

                curr_line = currStats[translation[prop]]

            idx = currLines.index[currLines['PROP'] == prop].to_list()[0]
            comparison_stat = currLines[currLines['PROP'] == prop]
            over_under = comparison_stat['OVER_UNDER'].iloc[0]
            prop_line = comparison_stat['LINE'].iloc[0]

            if over_under == "O":
                
                if prop_line < curr_line:
                    
                    result = 1
                    system.loc[idx, 'RESULT'] = 1
                
                else:
                    
                    result = 0
                    system.loc[idx, 'RESULT'] = 0
            
            if over_under == "U":
                
                if prop_line > curr_line:
                    
                    result = 1
                    system.loc[idx, 'RESULT'] = 1
                
                else:
                    
                    result = 0
                    system.loc[idx, 'RESULT'] = 0

            cursor.execute("""

                UPDATE SYSTEM
                SET RESULT = ?
                WHERE DATE = ?
                AND PLAYER_ID = ?
                AND PROP = ?
                        
                """, (result, date, pid, prop))

    conn.commit()

    system = system[

        (system['RESULT'] == 1) |
        (system['RESULT'] == 0)
        
    ].copy()

    date_dir_path = os.path.join(config.GAME_FILES_PATH, date,)

    if not os.path.isdir(date_dir_path):

        os.mkdir(date_dir_path)

    file_path = os.path.join(config.GAME_FILES_PATH, date, "system_grade.json")

    system.to_json(file_path, orient="records", indent=4)

    return system

def update_props_training_table(season_start_date, curr_date, conn):

    config = load_config()

    def find_minutes_projection(season_game_logs, curr_scoreboard, positions_df, season_start_date, curr_date, player_id, conn):

        def find_minute_projection_features(conn, season_start_date, curr_date, season_game_logs, curr_scoreboard, positions_df, player_id):

            def avg_last_3_5_7_10(game_logs, player_id, curr_date):

                game_logs = game_logs[
                    (game_logs['GAME_DATE'] < str(curr_date)) &
                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['MIN'] > 0)
                ]

                minutes_list = game_logs['MIN'].to_list()
                last_3 = []
                last_5 = []
                last_7 = []
                last_10 = []

                if len(minutes_list) == 0:

                    return np.nan, np.nan, np.nan, np.nan

                i = 0

                while i < min(10, len(minutes_list)):

                    if len(last_3) < 3:
                        last_3.append(minutes_list[i])       
                    if len(last_5) < 5:
                        last_5.append(minutes_list[i])
                    if len(last_7) < 7:
                        last_7.append(minutes_list[i])
                    if len(last_10) < 10:
                        last_10.append(minutes_list[i])

                    i += 1
                
                curr_average = sum(minutes_list) / len(minutes_list)

                while len(last_10) < 10:

                    if len(last_3) < 3:
                        last_3.append(curr_average)       
                    if len(last_5) < 5:
                        last_5.append(curr_average)
                    if len(last_7) < 7:
                        last_7.append(curr_average)
                    if len(last_10) < 10:
                        last_10.append(curr_average)

                average_last_3 = float(sum(last_3) / 3)
                average_last_5 = float(sum(last_5) / 5)
                average_last_7 = float(sum(last_7) / 7)
                average_last_10 = float(sum(last_10) / 10)

                return average_last_3, average_last_5, average_last_7, average_last_10

            def minutes_trend_5(curr_date, player_id, player_name, season_game_logs):
                
                curr_player_game_logs = season_game_logs[
                    (season_game_logs['GAME_DATE'] < str(curr_date)) &
                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['MIN'] > 0)
                ]
                
                minutes_list = curr_player_game_logs['MIN'].to_list()

                last_5 = []
                average = 0

                if len(minutes_list) == 0:

                    print(f"Could not find games with minutes before {curr_date} for {player_name}.")
                    return np.nan
                
                last_5 = minutes_list[:5]

                if len(last_5) < 2:

                    return np.nan
                
                average = sum(last_5) / len(last_5)

                while len(last_5) < 5:

                    last_5.append(average)

                slope = (last_5[-1] - last_5[0]) / 4

                return slope

            def find_position_missing_minutes(conn, curr_date, positions, team_id):

                dfs = []

                for position in positions:

                    df = pd.read_sql_query("""

                            SELECT d.*
                            FROM DNPS d
                            JOIN PLAYER_POSITIONS p
                                ON p.PLAYER_ID = d.PLAYER_ID
                            WHERE p.POSITION = ?
                            AND d.GAME_DATE = ?
                            AND d.TEAM_ID = ?
                                        
                        """, conn, params=(position, str(curr_date), team_id))

                    if not df.empty:
                        dfs.append(df)
                
                if not dfs:

                    return 0

                cat = pd.concat(dfs, ignore_index=True)

                total_pos_minutes = cat.drop_duplicates('PLAYER_ID')['AVERAGE_MINUTES'].sum()
                
                return min(48, total_pos_minutes)

            def find_last_10_std_dev(curr_date, player_id, player_name, game_logs):

                current_player_game_logs = game_logs[
                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['GAME_DATE'] < curr_date) &
                    (game_logs['MIN'] > 0)
                ]

                if current_player_game_logs.empty:

                    print(f"Could not find game logs before {curr_date} for {player_name}")
                    return np.nan

                last_10_games = current_player_game_logs.iloc[:10]['MIN'].to_list()

                if len(last_10_games) < 5:

                    return np.nan

                std_dev = statistics.stdev(last_10_games)

                return std_dev

            def find_days_rest(curr_date, player_id, game_logs, season_start_date_str):

                curr_player_game_logs = game_logs[

                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['GAME_DATE'] < curr_date) &
                    (game_logs['MIN'] > 0)
                ]

                last_game_date = datetime.strptime(curr_date, "%Y-%m-%d").date() - timedelta(days=1)
                season_start_date = datetime.strptime(season_start_date_str, "%Y-%m-%d").date()

                days_rest = 0

                while last_game_date >= season_start_date:

                    if not curr_player_game_logs[curr_player_game_logs['GAME_DATE'] == str(last_game_date)].empty:

                        return days_rest

                    days_rest += 1
                    last_game_date -= timedelta(days=1)
                
                return np.nan

            def find_total_games_played_this_season(curr_date, player_id, game_logs):

                current_player_season_game_logs = len(
                    game_logs[

                        (game_logs['PLAYER_ID'] == player_id) &
                        (game_logs['GAME_DATE'] < curr_date) &
                        (game_logs['MIN'] > 0)
                    ]
                )

                return current_player_season_game_logs

            def find_if_back_to_back(curr_date, player_id, game_logs):

                day_before_curr_date = datetime.strptime(curr_date, "%Y-%m-%d").date() - timedelta(days=1)

                if game_logs[

                    (game_logs['GAME_DATE'] == str(day_before_curr_date)) &
                    (game_logs['PLAYER_ID'] == player_id)

                ].empty:
                    
                    return 0

                return 1

            def find_games_started_last_5(curr_date, player_id, season_game_logs):

                player_game_logs = season_game_logs[
                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['GAME_DATE'] < curr_date) &
                    (season_game_logs['MIN'] > 0)
                ].iloc[:5]

                if player_game_logs.empty:

                    return np.nan

                return len(player_game_logs[
                    (player_game_logs['STARTER'] == 1)
                ])

            def find_games_played_last_5_10_compared_to_team(curr_date, player_id, season_game_logs):

                player_logs = season_game_logs[

                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['GAME_DATE'] < str(curr_date)) 
                    
                ].iloc[:10]

                if player_logs.empty:

                    return np.nan, np.nan

                team_id = player_logs['TEAM_ID'].iloc[0]

                team_logs = season_game_logs[

                    (season_game_logs['TEAM_ID'] == team_id) &
                    (season_game_logs['GAME_DATE'] < str(curr_date))

                ].drop_duplicates("GAME_DATE").iloc[:10]

                games_played_last_5 = 0
                games_played_last_10 = 0
                team_logs_game_id_list = team_logs['GAME_ID'].to_list()
                player_logs_game_id_list = player_logs['GAME_ID'].to_list()

                i = 0

                for gameId in team_logs_game_id_list:
                    
                    if gameId in player_logs_game_id_list:

                        if games_played_last_5 < 5:

                            games_played_last_5 += 1
                        
                        games_played_last_10 += 1
                        
                    i += 1

                    if i == 10:

                        break
                
                return games_played_last_5, games_played_last_10

            positions = positions_df[positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()
            team_id = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['TeamID'].iloc[0]
            player_name = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]

            print(f"Finding minutes projection features for {player_name}...")

            average_last_3, average_last_5, average_last_7, average_last_10 = avg_last_3_5_7_10(season_game_logs, player_id, str(curr_date))
            minute_trend = minutes_trend_5(str(curr_date), player_id, player_name, season_game_logs)
            position_missing_minutes = find_position_missing_minutes(conn, str(curr_date), positions, int(team_id))
            last_10_std_dev = find_last_10_std_dev(str(curr_date), player_id, player_name, season_game_logs)
            days_rest = find_days_rest(str(curr_date), player_id, season_game_logs, season_start_date)
            total_games_played_this_season = find_total_games_played_this_season(str(curr_date), player_id, season_game_logs)
            is_back_to_back = find_if_back_to_back(str(curr_date), player_id, season_game_logs)
            games_started_last_5 = find_games_started_last_5(str(curr_date), player_id, season_game_logs)
            games_played_last_5, games_played_last_10 = find_games_played_last_5_10_compared_to_team(str(curr_date), player_id, season_game_logs)

            features_dict = {
                "AVERAGE_LAST_3": average_last_3, 
                "AVERAGE_LAST_5": average_last_5, 
                "AVERAGE_LAST_7": average_last_7, 
                "AVERAGE_LAST_10": average_last_10, 
                "MINUTE_TREND": minute_trend, 
                "POSITION_MISSING_MINUTES": position_missing_minutes, 
                "LAST_10_STANDARD_DEVIATION": last_10_std_dev,
                "DAYS_OF_REST": days_rest,
                "TOTAL_GAMES_PLAYED_THIS_SEASON": total_games_played_this_season,
                "IS_BACK_TO_BACK": is_back_to_back,
                "GAMES_STARTED_LAST_5": games_started_last_5,
                "GAMES_PLAYED_LAST_5": games_played_last_5,
                "GAMES_PLAYED_LAST_10": games_played_last_10,
            }


            return features_dict

        minutes_projection_features = pd.DataFrame([find_minute_projection_features(
            conn=conn, 
            season_start_date=season_start_date, 
            curr_date=curr_date,
            season_game_logs=season_game_logs,
            curr_scoreboard=curr_scoreboard,
            positions_df=positions_df,
            player_id = player_id
        )])

        minutes_projection_model_path = os.path.join(config.XGBOOST_PATH, "minutes_projection_model.pkl")

        model = joblib.load(minutes_projection_model_path)

        minutes_projection = model.predict(minutes_projection_features)[0]

        return minutes_projection

    def find_team_totals_and_player_share(curr_game_logs, stat, team_totals, team_id):
            
        if curr_game_logs.empty:

            return np.nan, np.nan, np.nan, np.nan

        col_name = f"PCT_{stat}_USAGE"

        curr_game_logs = curr_game_logs.sort_values("GAME_DATE", ascending=False)

        last_5_player_games = curr_game_logs.iloc[:5]
        last_10_player_games = curr_game_logs.iloc[:10]

        all_team_ids = curr_game_logs.drop_duplicates("TEAM_ID")['TEAM_ID'].to_list()

        curr_team_totals_df_list = []

        for game_id in last_10_player_games['GAME_ID'].to_list():
            
            if isinstance(game_id, int):
                
                game_id = "00" + str(game_id)

            curr_game_team_total = team_totals[
                (team_totals['GAME_ID'] == game_id) &
                (team_totals['TEAM_ID'] == str(team_id))
            ]

            if curr_game_team_total.empty:
                
                for curr_team_id in all_team_ids:

                    curr_game_team_total = team_totals[
                        (team_totals['GAME_ID'] == game_id) &
                        (team_totals['TEAM_ID'] == str(curr_team_id))
                    ]

                    if not curr_game_team_total.empty:

                        break
            
            if curr_game_team_total.empty:
                print(f"Current Game ID Team Totals are empty..")
                print(game_id, all_team_ids)
                sys.exit(1)

            curr_team_totals_df_list.append(curr_game_team_total)

        if not curr_team_totals_df_list:

            print(f"Could not find team totals for {game_id} and team: {team_id}")
            sys.exit(1)
            
        curr_team_totals_df = pd.concat(curr_team_totals_df_list, ignore_index=True)

        avg_last_5_pct_share = float(last_5_player_games[col_name].sum()) / len(last_5_player_games[col_name])
        avg_last_10_pct_share = float(last_10_player_games[col_name].sum()) / len(last_10_player_games[col_name])

        team_totals_last_5 = curr_team_totals_df.iloc[:5]
        team_totals_last_10 = curr_team_totals_df.iloc[:10]

        avg_last_5_team_totals = float(team_totals_last_5[stat].sum()) / len(team_totals_last_5)
        avg_last_10_team_totals = float(team_totals_last_10[stat].sum()) / len(team_totals_last_10)

        return avg_last_5_pct_share, avg_last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals

    def find_defensive_rank(conn, positions, team_id, prop):

        rank = 0

        for position in positions:

            df = pd.read_sql_query(f"SELECT * FROM DEFENSE_VS_POSITION_2025_2026 WHERE POSITION = ? ORDER BY {prop} DESC", conn, params=(position,))

            rank += df.index[df['TEAM_ID'] == team_id][0]
        
        return rank / len(positions)

    def find_overall_games(player_game_logs_before_curr_date_overall, prop, prop_line):

        if player_game_logs_before_curr_date_overall.empty:

            print(f"Couldn't find player game logs before the overall date for {player_name}")
            return [], np.nan

        overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()

        average_L3_minus_line = sum(overall_games_for_current_prop[:3]) / len(overall_games_for_current_prop[:3]) - prop_line
        average_L5_minus_line = sum(overall_games_for_current_prop[:5]) / len(overall_games_for_current_prop[:5]) - prop_line
        average_L7_minus_line = sum(overall_games_for_current_prop[:7]) / len(overall_games_for_current_prop[:7]) - prop_line
        average_L10_minus_line = sum(overall_games_for_current_prop[:10]) / len(overall_games_for_current_prop[:10]) - prop_line
    
        if not overall_games_for_current_prop:

            return np.nan, np.nan

        average_overall_last_20_minus_line = sum(overall_games_for_current_prop[:20]) / len(overall_games_for_current_prop[:20]) - prop_line

        last_5_games_overall = []

        for i in range(min(5, len(overall_games_for_current_prop))):

            last_5_games_overall.append(overall_games_for_current_prop[i])

        return (
            last_5_games_overall, average_overall_last_20_minus_line,
            average_L3_minus_line, average_L5_minus_line,
            average_L7_minus_line, average_L10_minus_line
        )

    def find_opp_games(player_game_logs_before_curr_date_vs_opp, prop, prop_line):

        opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

        if player_game_logs_before_curr_date_vs_opp.empty:

            print(f"Could not find gamelogs for current player before {curr_date}")
            return [], np.nan, np.nan, np.nan, 0

        opp_games_for_current_prop = player_game_logs_before_curr_date_vs_opp[prop].to_list()

        average_L3_minus_line = sum(opp_games_for_current_prop[:3]) / len(opp_games_for_current_prop[:3]) - prop_line
        average_L7_minus_line = sum(opp_games_for_current_prop[:7]) / len(opp_games_for_current_prop[:7]) - prop_line
        average_L10_minus_line = sum(opp_games_for_current_prop[:10]) / len(opp_games_for_current_prop[:10]) - prop_line
        
        last_5_games_vs_opp_list = []

        for i in range(min(5, opp_game_count)):

            last_5_games_vs_opp_list.append(opp_games_for_current_prop[i])
        
        while len(last_5_games_vs_opp_list) < 5:

            last_5_games_vs_opp_list.append(None)
        
        return last_5_games_vs_opp_list, average_L3_minus_line, average_L7_minus_line, average_L10_minus_line, opp_game_count

    team_totals_df = pd.read_sql_query("SELECT * FROM TEAM_STATS_2025_2026", conn)
    
    cursor = conn.cursor()

    props = [
        'PTS',
        'REB',
        'AST',
        'STL',
        'BLK',
        'FG3M',
        'PRA',
        'PTS_REB',
        'PTS_AST',
        'REB_AST'
    ]

    translation = {

        "PTS": "player_points",
        "REB": "player_rebounds",
        "AST": "player_assists",
        "FG3M": "player_threes",
        "BLK": "player_blocks",
        "STL": "player_steals",
        "BLK_STL": "player_blocks_steals",
        "TOV": "player_turnovers",
        "PRA": "player_points_rebounds_assists",
        "PTS_REB": "player_points_rebounds",
        "PTS_AST": "player_points_assists",
        "REB_AST": "player_rebounds_assists",
        "FGM": "player_field_goals",
        "FTM": "player_frees_made",
        "FGA": "player_frees_attempt"

    }

    nofind_player_ids = []
    two_years_from_curr_date = str(curr_date - timedelta(days=730))

    #just curr_date in string form instead of date form
    game_date = str(curr_date)
    today = datetime.strftime(datetime.now(ZoneInfo(config.TIMEZONE)).date(), "%Y-%m-%d")

    # all games during and before the curr_date ordered by game date descending
    game_logs = pd.read_sql_query('SELECT * FROM player_game_logs WHERE GAME_DATE <= ? ORDER BY GAME_DATE DESC', conn, params=(str(curr_date),))

    if game_logs[game_logs['GAME_DATE'] == game_date].empty and game_date < today:

        print(f"Game logs for {curr_date} not found")
        sys.exit(1)

    system = pd.read_sql_query('SELECT * FROM SYSTEM WHERE DATE = ?', conn, params=(str(curr_date),))

    if system.empty:

        print(f"Could not find game props for {curr_date}..")
        curr_date += timedelta(days=1)
        return

    # used to find the games for curr_date
    scoreboard = pd.read_sql_query('SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?', conn, params=(str(curr_date),))
    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE > ? ORDER BY GAME_DATE DESC", conn, params=(season_start_date,))

    player_positions_df = pd.read_sql_query('SELECT * FROM PLAYER_POSITIONS', conn,)

    # finds all player ids for the players that are playing during curr_date
    player_ids = scoreboard.drop_duplicates('PLAYER_ID')['PLAYER_ID'].to_list()
    
    # iterates through all players playing during curr_date
    for player_id in player_ids:

        minutes_projection = find_minutes_projection(
            season_game_logs=season_game_logs, 
            curr_scoreboard=scoreboard, 
            positions_df=player_positions_df, 
            season_start_date=season_start_date, 
            curr_date=curr_date, 
            player_id=player_id,
            conn=conn
        )

        player_name = scoreboard[scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]
        team_id = scoreboard[scoreboard['PLAYER_ID'] == player_id]['TeamID'].iloc[0]
        matchup = (
            0
            if '@' in scoreboard[scoreboard['PLAYER_ID'] == player_id]['MATCHUP'].iloc[0]
            else 1
        )
        
        curr_player_game_logs = game_logs[game_logs['PLAYER_ID'] == player_id]

        if curr_player_game_logs[curr_player_game_logs['GAME_DATE'] == game_date].empty:
            
            print(f"Could not find a game log for {player_name} on {game_date}.")
            nofind_player_ids.append(player_id)
            continue

        # finds player positions
        player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

        # finds the game_id for the player's game today
        game_id = curr_player_game_logs[
            (curr_player_game_logs['GAME_DATE'] == game_date)
        ]['GAME_ID'].iloc[0]

        player_box_score = curr_player_game_logs[curr_player_game_logs['GAME_DATE'] == game_date]

        # opposition_id for the current matchup of curr_date
        opposition_id = scoreboard[scoreboard['PLAYER_ID'] == player_id]['opposition_team_id'].iloc[0]

        # finds player games against opp before current date
        player_game_logs_before_curr_date_vs_opp = curr_player_game_logs[
            (curr_player_game_logs['GAME_DATE'] < game_date) & # before curr date
            (curr_player_game_logs['OPPOSITION_ID'] == opposition_id) & # if the opposition id is the same as the scoreboard's
            (curr_player_game_logs['GAME_DATE'] >= two_years_from_curr_date) & # needs to be later than two years ago
            (curr_player_game_logs['MIN'] > 0) # needs to play more than 0 minutes
        ]

        # finds total number of games against opp
        opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

        # finds player games overall during the season
        player_game_logs_before_curr_date_overall = curr_player_game_logs[
            (curr_player_game_logs['GAME_DATE'] < game_date) & # before curr date
            (curr_player_game_logs['GAME_DATE'] >= season_start_date) & # needs to be during this season
            (curr_player_game_logs['MIN'] > 0) # needs to player more than 0 minutes
        ]

        games_played_this_season = len(player_game_logs_before_curr_date_overall)

        extra_pct_dict = {}

        for prop in props:
            
            last_5_pct_share, last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals = find_team_totals_and_player_share(
                curr_game_logs=player_game_logs_before_curr_date_overall, 
                stat=prop,
                team_totals=team_totals_df,
                team_id=team_id
            )
            
            current_player_prop_row = system[
                (system['PLAYER_ID'] == player_id) &
                (system['PROP'] == translation[prop])
            ]

            if current_player_prop_row.empty:

                continue

            prop_line = current_player_prop_row['LINE'].iloc[0]

            if np.isnan(minutes_projection):

                expected_from_last_5_minus_line = np.nan
                expected_from_last_10_minus_line = np.nan
            
            else:

                expected_from_last_5_minus_line = ((avg_last_5_team_totals / 240) * minutes_projection * last_5_pct_share) - prop_line
                expected_from_last_10_minus_line = ((avg_last_10_team_totals / 240) * minutes_projection * last_10_pct_share) - prop_line


            extra_pct_dict[f"AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE"] = expected_from_last_5_minus_line
            extra_pct_dict[f"AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE"] = expected_from_last_10_minus_line

            stat_line_for_current_game = player_box_score[prop].iloc[0]

            if stat_line_for_current_game > prop_line:

                result = 1
            
            else:

                result = 0

            print(f"Updating the PROPS_TRAINING_TABLE for {player_name} on {game_date} for prop {prop}...")
            # defensive rank based on team and statline

            def_rank = find_defensive_rank(conn, player_positions, opposition_id, prop)
            (
                last_5_games_overall, average_overall_last_20_minus_line,
                average_L3_overall_minus_line, average_L5_overall_minus_line,
                average_L7_overall_minus_line, average_L10_overall_minus_line
            ) = find_overall_games(player_game_logs_before_curr_date_overall, prop, prop_line)
            
            (
                last_5_games_vs_opp, average_L3_vs_opp_minus_line, 
                average_L7_vs_opp_minus_line, average_L10_vs_opp_minus_line, 
                opp_game_count
            ) = find_opp_games(player_game_logs_before_curr_date_vs_opp, prop, prop_line)

            last_game = (
                last_5_games_overall[0] - prop_line
                if len(last_5_games_overall) > 0
                else np.nan
            )
            second_last_game = (
                last_5_games_overall[1] - prop_line
                if len(last_5_games_overall) > 1
                else np.nan
            )
            third_last_game = (
                last_5_games_overall[2] - prop_line
                if len(last_5_games_overall) > 2
                else np.nan
            )
            fourth_last_game = (
                last_5_games_overall[3] - prop_line
                if len(last_5_games_overall) > 3
                else np.nan
            )
            fifth_last_game = (
                last_5_games_overall[4] - prop_line
                if len(last_5_games_overall) > 4
                else np.nan
            )
            last_game_vs_opp = (
                last_5_games_vs_opp[0] - prop_line
                if len(last_5_games_vs_opp[0]) > 0
                else np.nan
            )
            second_last_game_vs_opp = (
                last_5_games_vs_opp[1] - prop_line
                if len(last_5_games_vs_opp[1]) > 1
                else np.nan
            )
            third_last_game_vs_opp = (
                last_5_games_vs_opp[2] - prop_line
                if len(last_5_games_vs_opp[2]) > 2
                else np.nan
            )
            fourth_last_game_vs_opp = (
                last_5_games_vs_opp[3] - prop_line
                if len(last_5_games_vs_opp[3]) > 3
                else np.nan
            )
            fifth_last_game_vs_opp = (
                last_5_games_vs_opp[4] - prop_line
                if len(last_5_games_vs_opp[4]) > 4
                else np.nan
            )

            training_table_row = {
                'GAME_DATE': game_date,
                'GAME_ID': game_id,
                'PLAYER_NAME': player_name,
                'PLAYER_ID': player_id,
                'PROP': prop,
                'PROP_LINE': prop_line,
                'LAST_GAME': last_game,
                'SECOND_LAST_GAME': second_last_game,
                'THIRD_LAST_GAME': third_last_game,
                'FOURTH_LAST_GAME': fourth_last_game,
                'FIFTH_LAST_GAME': fifth_last_game,
                'AVERAGE_LAST_20': average_overall_last_20_minus_line,
                'LAST_GAME_VS_OPP': last_game_vs_opp,
                'SECOND_LAST_GAME_VS_OPP': second_last_game_vs_opp,
                'THIRD_LAST_GAME_VS_OPP': third_last_game_vs_opp,
                'FOURTH_LAST_GAME_VS_OPP': fourth_last_game_vs_opp,
                'FIFTH_LAST_GAME_VS_OPP': fifth_last_game_vs_opp,
                'AVERAGE_LAST_10_VS_OPP': average_L10_vs_opp_minus_line,
                'AVG_LAST_3_VS_OPP': average_L3_vs_opp_minus_line,
                'AVG_LAST_7_VS_OPP': average_L7_vs_opp_minus_line,
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
                'TARGET': int(result),
                'VENUE': matchup,
                'MINUTES_PROJECTION': minutes_projection,
                'AVG_LAST_3_OVERALL': average_L3_overall_minus_line,
                'AVG_LAST_5_OVERALL': average_L5_overall_minus_line,
                'AVG_LAST_7_OVERALL': average_L7_overall_minus_line,
                'AVG_LAST_10_OVERALL': average_L10_overall_minus_line,
                'MINUTES_PROJECTION': minutes_projection,
                'GAMES_PLAYED_THIS_SEASON': games_played_this_season
            }

            placeholders = ", ".join(['?']*len(training_table_row))
            columns = ", ".join(training_table_row.keys())
            stats = list(training_table_row.values())

            query = f"INSERT OR REPLACE INTO PROPS_TRAINING_TABLE ({columns}) VALUES ({placeholders})"

            cursor.execute(query, stats)
        
        for k, v in extra_pct_dict.items():

            cursor.execute(f"UPDATE PROPS_TRAINING_TABLE SET {k} = ? WHERE GAME_DATE = ? AND PLAYER_ID = ?", (v, str(curr_date), player_id))
    
    conn.commit()

if __name__ == "__main__":

    config = load_config()
    
    API_KEY = config.API_KEY

    conn = sqlite3.connect(config.DB_PATH)

    current_season = "2025-26"
    current_season_start_date = "2025-10-21"

    curr_date_str = "2026-01-05"
    end_date_str = "2026-01-05"
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    while curr_date <= end_date:
        
        current_path = os.path.join(config.GAME_FILES_PATH, str(curr_date))

        check_for_existing = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE GAME_DATE = ?", conn, params=(str(curr_date),))

        if not check_for_existing.empty:

            print(f"Already have data for {curr_date}..")
            curr_date += timedelta(days=1)
            continue

        if not os.path.isdir(current_path):

            os.mkdir(current_path)

        current_ids = get_odds_api_ids(API_KEY, str(curr_date), conn)

        if not current_ids:

            print(f"No games found for {curr_date}")
            curr_date += timedelta(days=1)
            continue

        game_props = get_historical_prop_lines(str(curr_date), current_ids, API_KEY, conn)

        draftkings_sportsbook = props_parser(game_props, conn, str(curr_date))

        scoboard_to_team_roster_df = scoreboard_to_team_roster(current_season, str(curr_date), conn)

        player_vs_team_or_last_20_df = player_vs_team_or_last_20(scoboard_to_team_roster_df, str(curr_date), current_season_start_date, conn)

        scores = player_vs_prop_scores(player_vs_team_or_last_20_df, draftkings_sportsbook, str(curr_date), conn, current_season_start_date)

        result(scores, str(curr_date), conn)

        system_grade(str(curr_date), conn)

        update_props_training_table(current_season_start_date, curr_date, conn)

        curr_date += timedelta(days=1)
    
    print(f"Updated sql tables from {curr_date_str} - {end_date_str}.")
