import json
import time
import pandas as pd
import os
import sqlite3
import sys
from datetime import datetime, timedelta
import unicodedata
from io import StringIO
from nba_api.live.nba.endpoints import boxscore
import joblib
import numpy as np
import statistics

from basketball_stats_bot.config import load_config
from basketball_stats_bot.programs.scoring.scoring_functions import (
    scoringv1,
    scoringv2,
    scoringv3,
    scoringv4,
    scoringv5,
    scoringv6,
    scoringv7,
    scoringv8,
    scoringv9
)

def player_vs_prop_scores(player_vs_team_or_last_20_df, draftkings_sportsbook, date, conn, functions):

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
        'Derrick Jones': 'Derrick Jones Jr'
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
    

    for function_name in functions:

        system[function_name] = {}

        for player, prop_lines in draftkings_sportsbook.items():

            print(f"Calculating score for {player} using {function_name}")

            if player in name_edge_cases:

                player = name_edge_cases[player]

            if player not in system:

                system[function_name][player] = {}

            if player not in player_games:

                print(f"Could not find {player} in player_games")
                sys.exit(1)
                continue

            curr_player_vs_team_or_last_20_game_logs = player_games[player]

            curr_player_vs_team_or_last_20_game_logs = curr_player_vs_team_or_last_20_game_logs.sort_values('GAME_DATE', ascending=False)

            if curr_player_vs_team_or_last_20_game_logs.empty:

                continue
            
            current_opposition_ID = scoreboard[scoreboard["PLAYER"] == player]['opposition_team_id'].iloc[0]
            player_id = scoreboard[scoreboard['PLAYER'] == player]['PLAYER_ID'].iloc[0]

            minutes_projection = find_minutes_projection(season_game_logs=season_game_logs, 
                                                         curr_scoreboard=scoreboard, 
                                                         positions_df=player_positions_df, 
                                                         season_start_date=season_start_date, 
                                                         curr_date=date, 
                                                         player_id=player_id, 
                                                         conn=conn
                                                        )

            for prop, line in prop_lines.items():
                
                if int(function_name[-1]) >= 4:
                    
                    if function_name[-1] in {'7', '8', '9'}:
                        curr_score = functions[function_name](curr_player_vs_team_or_last_20_game_logs, current_opposition_ID, translation[prop], line, scoreboard, player_positions_df, date, team_totals_df, minutes_projection, season_game_logs)
                    else:
                        curr_score = functions[function_name](curr_player_vs_team_or_last_20_game_logs, current_opposition_ID, translation[prop], line, scoreboard, player_positions_df, date)

                    if curr_score == -2:

                        continue
                    
                else:

                    curr_score = functions[function_name](curr_player_vs_team_or_last_20_game_logs, current_opposition_ID, translation[prop], line)

                system[function_name][player][prop] = (curr_score, line)
                system[function_name][player]['PERSON_ID'] = int(curr_player_vs_team_or_last_20_game_logs['PLAYER_ID'].iloc[0])

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

        def insert_reverse_sorted(self, player_name, prop, score, line, matchup, curr_date):

            if self.head and score == None:

                return

            if score < 0:

                over_under = "U"
            
            elif score > 0:

                over_under = "O"
            
            else:

                over_under = "O/U"
            
            now = time.strftime(f"%Y-%m-%d %H:%M:%S")

            node = Node({
                'date': curr_date,
                "player": player_name, 
                "over_under": over_under, 
                "prop": prop, 
                "line": line, 
                "matchup": matchup,
                "score": abs(score),
                "Last updated": now
                })

            prev = None
            curr = self.head

            while curr and (curr.val['score'] > node.val['score']):

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

    result_dict = {}

    rosters = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE DATE = ?", conn, params=(date,))

    for type_of_scoring, scores in scores.items():

        result_linked_list = LinkedList()

        for player_name, score_dict in scores.items():
            
            print(f"Adding {player_name} to the system..")

            for prop, score in score_dict.items():

                if prop == 'PERSON_ID':
                    continue

                currScore, line = score
                
                curr = rosters[rosters['PLAYER_ID'] == score_dict["PERSON_ID"]]

                matchup = curr['MATCHUP'].to_list()[0]

                result_linked_list.insert_reverse_sorted(player_name, prop, currScore, line, matchup, date)

        system_sorted = result_linked_list.to_array()

        result_dict[type_of_scoring] = pd.DataFrame(system_sorted)

        print(f"Created a system for {date} using {type_of_scoring} scoring")

    return result_dict

def system_grade(date, system, conn):

    def remove_accents(text):

        return "".join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != "Mn"
        )
    
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM NBA_API_GAME_IDS WHERE DATE = ?", (str(date),))
    fetch = cursor.fetchall()

    today_nba_api_game_ids = [gameId for curr_date, gameId in fetch]

    allteamboxscores = []

    for gameId in today_nba_api_game_ids:

        box = boxscore.BoxScore(gameId)

        stats = box.get_dict()['game']

        allteamboxscores.append(pd.DataFrame(stats['homeTeam']['players']))
        allteamboxscores.append(pd.DataFrame(stats['awayTeam']['players']))

    today_box_scores = pd.concat(allteamboxscores, ignore_index=True)

    player_names = today_box_scores['name'].to_list()

    for type_of_scoring, df in system.items():

        df["RESULT"] = pd.NA

        translation = {
            "player_points": 'points',
            "player_rebounds": 'reboundsTotal',
            "player_assists": "assists",
            "player_threes": "threePointersMade",
            "player_blocks": "blocks",
            "player_steals": "steals",
        }

        extra = {
            "player_points_rebounds_assists",
            "player_points_rebounds",
            "player_points_assists",
            "player_rebounds_assists",
        }

        for player in player_names:

            print(f"Grading {player}...")
            currStats = today_box_scores[today_box_scores['name'] == player]['statistics'].iloc[0]

            currLines = df[df['player'] == remove_accents(player)]

            props = currLines['prop'].to_list()

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

                # finds the index of where the prop is
                idx = currLines.index[currLines['prop'] == prop].to_list()[0]

                # finds the prop
                comparison_stat = currLines[currLines['prop'] == prop]

                # finds 
                over_under = comparison_stat['over_under'].iloc[0]
                prop_line = comparison_stat['line'].iloc[0]

                if over_under == "O":
                    
                    if prop_line < curr_line:
                        
                        df.loc[idx, 'RESULT'] = 1
                    
                    else:

                        df.loc[idx, 'RESULT'] = 0
                
                if over_under == "U":
                    
                    if prop_line > curr_line:
                        
                        df.loc[idx, 'RESULT'] = 1
                    
                    else:

                        df.loc[idx, 'RESULT'] = 0

        # this is if the score was 0 then the over_under category = O/U because it could literally go either way
        df = df[

            (df['RESULT'] == 1) |
            (df['RESULT'] == 0)
            
        ].copy()

    return system

def write_results(grades_dict, curr_date):

    results_text = []

    for type_of_scoring, df in grades_dict.items():

        curr_system_df_path = os.path.join(config.TESTING_RESULTS_DF_PATH, f"{str(curr_date)}_{type_of_scoring}_df.json")\
    
        df.to_json(curr_system_df_path, orient='records', indent=4)
        
        if int(type_of_scoring[-1]) >= 4:

            df = df[df['score'] >= 65]

        else:

            df = df[df["score"] >= 70]

        if len(df) >= 10:
            
            last_10 = df.iloc[:10]
            last_10_hit = len(last_10[last_10['RESULT'] == 1])

        if len(df) >= 20:

            last_20 = df.iloc[:20]
            last_20_hit = len(last_20[last_20['RESULT'] == 1][:20])

        if len(df) >= 50:

            last_50 = df.iloc[:50]
            last_50_hit = len(last_50[last_50['RESULT'] == 1][:50])

        if len(df) >= 100:

            last_100 = df.iloc[:100]
            last_100_hit = len(last_100[last_100['RESULT'] == 1][:100])

        overall_hit = len(df[df['RESULT'] == 1])

        total_length = len(df)
        
        curr_text = []

        if int(type_of_scoring[-1]) >= 4:

            text = f"\nThere were a total of {total_length} props that had a score above 65 during {str(curr_date)} using {type_of_scoring}\n"
        else:

            text = f"\nThere were a total of {total_length} props that had a score above 70 during {str(curr_date)} using {type_of_scoring}\n"   

        curr_text.append(text)
        print(text)

        if len(df) >= 10:

            text = f"\nIn the top 10 props you went {(last_10_hit / 10)*100:.2f}%\n"
            curr_text.append(text)
            print(text)

        if len(df) >= 20:

            text = f"\nIn the top 20 props you went {(last_20_hit / 20)*100:.2f}%\n"
            curr_text.append(text)
            print(text)

        if len(df) >= 50:

            text = f"\nIn the top 50 props you went {(last_50_hit / 50*100):.2f}%\n"
            curr_text.append(text)
            print(text)

        if len(df) >= 100:
            
            text = f"\nIn the top 100 props you went {(last_100_hit / 100)*100:.2f}%\n"
            curr_text.append(text)
            print(text)
        
        if overall_hit > 0:

            text = f"\nIn {total_length} props you went {(overall_hit / total_length)*100:.2f}%\n"
            curr_text.append(text)
            print(text)

        results_text.append("".join(curr_text))
    
    final_result_text = "\n".join(results_text)

    curr_date_testing_results_path = os.path.join(config.TESTING_RESULTS_PATH, f"{curr_date}_grade.txt")

    with open(curr_date_testing_results_path, "w") as f:

        f.write(final_result_text)

if __name__ == "__main__":

    config = load_config()

    functions = {

        # 'scoringv1': scoringv1,
        # 'scoringv2': scoringv2,
        # 'scoringv3': scoringv3,
        # 'scoringv4': scoringv4,
        # 'scoringv5': scoringv5,
        # 'scoringv6': scoringv6,
        'scoringv8': scoringv8,
        'scoringv9': scoringv9

    }

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)
    cursor = conn.cursor()

    curr_date_str = '2025-12-20'
    end_date_str = '2025-12-25'
    curr_date = datetime.strptime(curr_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

    translation = {
        'PTS': 'player_points',
        'REB': 'player_rebounds',
        'AST': 'player_assists',
        'FG3M': 'player_threes',
        'BLK': 'player_blocks',
        'STL': 'player_steals',
        'PRA': 'player_points_rebounds_assists',
        'PTS_REB': 'player_points_rebounds',
        'PTS_AST': 'player_points_assists',
        'REB_AST': 'player_rebounds_assists'
    }

    season_start_date = "2025-10-21"

    while curr_date <= end_date:

        print(f"Finding results for {str(curr_date)}..\n")

        check_for_json = pd.read_sql_query("SELECT * FROM PLAYER_VS_TEAM_OR_LAST_20_JSONS WHERE DATE = ?", conn, params=(str(curr_date),))

        if not check_for_json.empty:

            player_vs_team_or_last_20_df = pd.read_json(StringIO(check_for_json['JSON_FILE'].iloc[0]))
        
        else:

            print(f"Could not find player_vs_team_or_last_20 json file for {curr_date}")
            curr_date += timedelta(days=1)
            continue

        player_props = pd.read_sql_query("SELECT * FROM PLAYER_PROPS WHERE DATE = ?", conn, params=(str(curr_date),))

        player_names = player_props['PLAYER'].to_list()

        draftkings_sportsbook = {}

        for player in player_names:

            curr = player_props[player_props["PLAYER"] == player].to_dict()
            
            curr = {k: v for k, v in curr.items() if not pd.isna(list(v.values())[0])}

            draftkings_sportsbook[player.replace(".", "")] = {}

            for col, row in curr.items():

                if col not in {'DATE', 'PLAYER', 'PLAYER_ID'}:
                    
                    for key, val in row.items():
                        
                        draftkings_sportsbook[player.replace(".", "")][translation[col]] = val

        scores = player_vs_prop_scores(player_vs_team_or_last_20_df, draftkings_sportsbook, str(curr_date), conn, functions)

        result_df = result(scores, str(curr_date), conn)

        system_grade_df = system_grade(curr_date, result_df, conn)

        write_results(system_grade_df, curr_date)

        curr_date += timedelta(days=1)
    
    print(f"Finished testing between {curr_date_str} - {end_date_str}")
