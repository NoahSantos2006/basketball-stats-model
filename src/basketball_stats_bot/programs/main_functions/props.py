import requests
import json
import pandas as pd
import sys
import os
import sqlite3
from io import StringIO
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, date
import unicodedata
import numpy as np
import joblib
import statistics

from basketball_stats_bot.config import load_config
from basketball_stats_bot.programs.scoring.scoring_functions import scoringv9, scoringv10

def get_today_ids(API_KEY, conn):

    config = load_config()

    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/events"
    cursor = conn.cursor()
    today_game_ids = []

    # makes because the time set for the ids is in is 8601 format, we have to make sure the games are only showing today's
    today = datetime.now(ZoneInfo(config.TIMEZONE)).date()
    today_str = datetime.strftime(today, "%Y-%m-%d")
    tomorrow = today + timedelta(days=1)

    check_for_existing = pd.read_sql_query("SELECT * FROM ODDS_API WHERE DATE = ?", conn, params=(str(today),))

    if not check_for_existing.empty:

        print(f"Already found odds-api game ids for {today}")
        return check_for_existing['GAME_ID'].to_list()
    
    params = {
        "apiKey": API_KEY,
        'commenceTimefrom': f"{today}T05:00:00Z",
        "commenceTimeTo": f"{tomorrow}T04:59:59Z"
    }

    response = requests.get(url, params=params)
    data = response.json()

    today_game_ids = []

    for game in data: 

        today_game_ids.append(game['id'])

        cursor.execute("""

            INSERT OR REPLACE INTO ODDS_API (DATE, GAME_ID)
            VALUES (?, ?)
            
        """, (str(today_str), game['id']))
    
    conn.commit()

    dir_path = os.path.join(config.GAME_FILES_PATH, str(today_str))

    if not os.path.isdir(dir_path):
    
        os.mkdir(dir_path)

    file_path = os.path.join(config.GAME_FILES_PATH, str(today_str), "odds-api_game_ids.json")

    with open(file_path, "w") as f:

        json.dump(today_game_ids, f, indent=4)

    return today_game_ids

def get_prop_lines(date, today_ids, apiKey, conn):

    config = load_config()

    # potential player props = [
    #     "player_points",
    #     "player_rebounds",
    #     "player_assists",
    #     "player_threes",
    #     "player_blocks",
    #     "player_steals",
    #     "player_blocks_steals",
    #     "player_turnovers",
    #     "player_points_rebounds_assists",
    #     "player_points_rebounds",
    #     "player_points_assists"
    #     "player_rebounds_assists",
    #     "player_field_goals",
    #     "player_frees_made",
    #     "player_frees_attempts",
    # ]

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

    df = pd.read_sql_query("SELECT * FROM ODDS_API WHERE DATE = ?", conn, params=(str(date),))

    if not df.empty:

        if not df['GAME_PROPS'].iloc[0] == None:

            user_input = input(f"Already found props for {date}. Would you like to update the props? y/n: ").lower()

            while user_input not in {"y", "n"}:
                
                user_input = input(f"Already found props for {date}. Would you like to update the props? y/n: ").lower()
            
            if user_input == "n":

                return df
    
    print(f"Finding props for {date}...\n")

    for gameID in today_ids:
        
        url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{gameID}/odds"

        for prop in player_props:

            params = {

                "apiKey": apiKey,
                "regions": "us",
                "markets": prop,
                "odds_format": "american"

            }

            print(f"Retrieving {prop} from {gameID}...")

            response = requests.get(url, params=params)

            data = response.json()

            if 'message' in data:

                print(f"{data['message']}")
                sys.exit(1)


            df = pd.DataFrame(data)

            dfs.append(df)
    
    if not dfs:

        print(f"Game props dataframe is empty. Check game_props.py")
        sys.exit(1)

    game_props_df = pd.concat(dfs, ignore_index=True)
    
    file_path = os.path.join(config.GAME_FILES_PATH, str(date), "player_props.json")

    game_props_df.to_json(file_path, orient="records", indent=4)

    cursor = conn.cursor()

    cursor.execute("""

        UPDATE ODDS_API
        SET GAME_PROPS = ?
        WHERE DATE = ?

        """, (game_props_df.to_json(), str(date)))
    
    conn.commit()

    return game_props_df

def props_parser(all_game_event_odds, conn):

    config = load_config()
    
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
        'Derrick Jones': 'Derrick Jones Jr',
        'Paul Reed Jr': 'Paul Reed'
    }

    print(f"Parsing props...")
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
    
    date = datetime.now(ZoneInfo(config.TIMEZONE)).date()

    game_logs_player_names = pd.read_sql_query("SELECT * FROM player_game_logs", conn).drop_duplicates("PLAYER_ID")

    for player_name, props in parser.items():

        name_cleaned = clean_name(player_name)

        if player_name in name_edge_cases:

            name_cleaned = name_edge_cases[player_name]

        player_game_logs = game_logs_player_names[game_logs_player_names['NAME_CLEAN'] == name_cleaned]

        if player_game_logs.empty:

            print(f"Could not find game logs for {player_name} (Check props.py Line 293)")
            sys.exit(1)

        player_id = int(player_game_logs['PLAYER_ID'].iloc[0])
            
        curr_column = ['DATE', 'PLAYER', 'PLAYER_ID']
        curr_values = [str(date), player_name, player_id]

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

                game_logs = game_logs.sort_values("GAME_DATE", ascending=False)

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

        return float(minutes_projection)

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
    team_totals_per_player_df = pd.read_sql_query("SELECT * FROM TEAM_TOTALS_PER_PLAYER", conn)
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

        print(f"Calculating score for {player} using scoringv9")

        player = clean_name(player)

        if player in name_edge_cases:

            player = name_edge_cases[player]

        if player not in system:

            system[player] = {}

        if clean_name(player) not in player_games:

            print(f"Could not find {player} in player_games")
            sys.exit(1)
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
                            
            curr_score = scoringv10(curr_player_vs_team_or_last_20_df, current_opposition_ID, 
                                   translation[prop], line, scoreboard, 
                                   player_positions_df, date, team_totals_per_player_df, 
                                   minutes_projection, season_game_logs, conn)

            if curr_score == -2 or not curr_score:

                continue

            system[player][prop] = (float(curr_score), line)
            system[player]['PERSON_ID'] = int(player_vs_team_or_last_20_df[player_vs_team_or_last_20_df['NAME_CLEAN'] == player]['PLAYER_ID'].iloc[0])
    
    file_path = os.path.join(config.GAME_FILES_PATH, str(date), "scores.json")

    with open(file_path, "w") as f:

        json.dump(system, f, indent=4)

    return system
 
if __name__ == "__main__":

    config = load_config()
    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)
    cursor = conn.cursor()

    curr_date = "2026-01-01"

    curr_path = r"C:\Users\noahs\.vscode\basketball_stats_model\src\basketball_stats_bot\data\game_files\2026-01-01"
    odds_api_game_ids_path = os.path.join(curr_path, "odds-api_game_ids.json")

    with open(odds_api_game_ids_path, "r") as f:

        curr_ids = json.load(f)

    player_vs_prop_path = os.path.join(curr_path, "player_props.json")

    with open(player_vs_prop_path, "r") as f:

        all_game_event_odds = pd.DataFrame(json.load(f))
    
    cursor.execute("""

        UPDATE ODDS_API
        SET GAME_PROPS = ?
        WHERE DATE = ?

        """, (all_game_event_odds.to_json(), curr_date))
    
    conn.commit()