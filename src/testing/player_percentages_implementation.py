import sqlite3
import json
from io import StringIO
import pandas as pd
from datetime import datetime, date, timedelta
import numpy as np
import statistics
from zoneinfo import ZoneInfo
import joblib
import os
import sys
from nba_api.stats.endpoints import boxscoreusagev3

from basketball_stats_bot.config import load_config

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
    last_5_player_games = curr_game_logs.iloc[:5]
    last_10_player_games = curr_game_logs.iloc[:10]

    curr_team_totals_df_list = []

    for game_id in last_10_player_games['GAME_ID'].to_list():

        curr_team_totals_df_list.append(team_totals[
            (team_totals['GAME_ID'] == game_id) &
            (team_totals['TEAM_ID'] == str(team_id))
        ])
    
    if not curr_team_totals_df_list:

        print(f"Could not find team totals for {game_id} and team: {team_id}")

    curr_team_totals_df = pd.concat(curr_team_totals_df_list, ignore_index=True)

    avg_last_5_pct_share = float(last_5_player_games[col_name].sum()) / len(last_5_player_games[col_name])
    avg_last_10_pct_share = float(last_10_player_games[col_name].sum()) / len(last_10_player_games[col_name])

    team_totals_last_5 = curr_team_totals_df.iloc[:5]
    team_totals_last_10 = curr_team_totals_df.iloc[:10]

    avg_last_5_team_totals = float(team_totals_last_5[stat].sum()) / len(team_totals_last_5)
    avg_last_10_team_totals = float(team_totals_last_10[stat].sum()) / len(team_totals_last_10)

    return avg_last_5_pct_share, avg_last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()
    
    season_start_date = "2025-10-21"

    season_team_stats = pd.read_sql_query("SELECT * FROM TEAM_STATS_2025_2026", conn)
    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ? ORDER BY GAME_DATE DESC", conn, params=(season_start_date,))
    positions_df = pd.read_sql_query("SELECT * FROM PLAYER_POSITIONS", conn)
    scoreboard = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER", conn)
    team_totals_df = pd.read_sql_query("SELECT * FROM TEAM_STATS_2025_2026", conn)
    player_props_df = pd.read_sql_query("SELECT * FROM PLAYER_PROPS", conn)

    stats = [
        'PTS',
        'REB',
        'AST',
        'STL',
        'BLK',
        'FG3M'
    ]

    start_date_str = "2025-11-01"
    end_date_str = "2025-12-27"
    curr_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    while curr_date <= end_date:

        curr_scoreboard = scoreboard[scoreboard['date'] == str(curr_date)]

        if curr_scoreboard.empty:

            print(f"Could not find scoreboard data for {curr_date}")
            curr_date += timedelta(days=1)
            continue

        current_game_logs = season_game_logs[
            (season_game_logs['GAME_DATE'] < str(curr_date)) &
            (season_game_logs['MIN'] > 0)
        ]

        curr_player_ids = curr_scoreboard.drop_duplicates("PLAYER_ID")['PLAYER_ID'].to_list()

        for player_id in curr_player_ids:

            minutes_projection = find_minutes_projection(
                season_game_logs=season_game_logs, 
                curr_scoreboard=curr_scoreboard, 
                positions_df=positions_df, 
                season_start_date=season_start_date, 
                curr_date=curr_date, 
                player_id=player_id,
                conn=conn
            )

            curr_player_game_logs = current_game_logs[current_game_logs['PLAYER_ID'] == player_id]

            if not curr_player_game_logs.empty:
                
                team_id = curr_player_game_logs['TEAM_ID'].iloc[0]
                player_name = curr_player_game_logs['PLAYER_NAME'].iloc[0]
                print(f"Updating expected stat minue line for {player_name} on {curr_date}")

            for stat in stats:

                prop_line_row = player_props_df[
                    (player_props_df['PLAYER_ID'] == player_id) &
                    (player_props_df['DATE'] == str(curr_date))
                ]

                if prop_line_row.empty:

                    # print(f"Could not find a line for {player_id} {stat}")
                    continue
                    
                prop_line = float(prop_line_row[stat].iloc[0])
                print(f"Finding expected stat - prop_line for {player_name} {stat}")

                last_5_pct_share, last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals = find_team_totals_and_player_share(
                    curr_game_logs=curr_player_game_logs, 
                    stat=stat,
                    team_totals=team_totals_df,
                    team_id=team_id
                )

                if np.isnan(minutes_projection):

                    expected_from_last_5_minus_line = np.nan
                    expected_from_last_10_minus_line = np.nan
                
                else:

                    expected_from_last_5_minus_line = ((avg_last_5_team_totals / 240) * minutes_projection * last_5_pct_share) - prop_line
                    expected_from_last_10_minus_line = ((avg_last_10_team_totals / 240) * minutes_projection * last_10_pct_share) - prop_line

                curr_col_name_5 = f"AVERAGE_LAST_5_EXPECTED_{stat}_MINUS_LINE"
                curr_col_name_10 = f"AVERAGE_LAST_10_EXPECTED_{stat}_MINUS_LINE"

                cursor.execute(f"""
                               
                                UPDATE TRAINING_TABLE_WITH_NAN 
                                SET
                                    {curr_col_name_5} = ?,
                                    {curr_col_name_10} = ?
                                WHERE PLAYER_ID = ?
                                AND GAME_DATE = ?

                               """, (float(expected_from_last_5_minus_line), float(expected_from_last_10_minus_line), player_id, str(curr_date)))
                
                print(f"Updated Average last n expected {stat} minus line")

        curr_date += timedelta(days=1)
    
    conn.commit()
    print(f"Finished updating average percentage shares")