import sqlite3
import pandas as pd
import joblib
import os
import numpy as np
import statistics
from datetime import datetime, timedelta

from basketball_stats_bot.config import load_config


def find_points_projection(season_game_logs, curr_scoreboard, positions_df, season_start_date, curr_date, player_id, conn):

    def find_points_projection_features(conn, season_start_date, curr_date, season_game_logs, curr_scoreboard, positions_df, player_id):

        def avg_last_3_5_7_10(game_logs, player_id, curr_date):

            game_logs = game_logs[
                (game_logs['GAME_DATE'] < str(curr_date)) &
                (game_logs['PLAYER_ID'] == player_id) &
                (game_logs['MIN'] > 0)
            ]

            game_logs = game_logs.sort_values("GAME_DATE", ascending=False)

            minutes_list = game_logs['PTS'].to_list()
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

        def points_trend_5(curr_date, player_id, player_name, season_game_logs):
            
            curr_player_game_logs = season_game_logs[
                (season_game_logs['GAME_DATE'] < str(curr_date)) &
                (season_game_logs['PLAYER_ID'] == player_id) &
                (season_game_logs['MIN'] > 0)
            ]
            
            minutes_list = curr_player_game_logs['PTS'].to_list()

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

        def find_position_missing_pts(conn, curr_date, positions, team_id):

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

            total_pos_minutes = cat.drop_duplicates('PLAYER_ID')['AVERAGE_PTS'].sum()
            
            return total_pos_minutes

        def find_last_10_std_dev(curr_date, player_id, player_name, game_logs):

            current_player_game_logs = game_logs[
                (game_logs['PLAYER_ID'] == player_id) &
                (game_logs['GAME_DATE'] < curr_date) &
                (game_logs['MIN'] > 0)
            ]

            if current_player_game_logs.empty:

                print(f"Could not find game logs before {curr_date} for {player_name}")
                return np.nan

            last_10_games = current_player_game_logs.iloc[:10]['PTS'].to_list()

            if len(last_10_games) < 5:

                return np.nan

            std_dev = statistics.stdev(last_10_games)

            return std_dev

        def find_total_games_played_this_season(curr_date, player_id, game_logs):

            current_player_season_game_logs = len(
                game_logs[

                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['GAME_DATE'] < curr_date) &
                    (game_logs['MIN'] > 0)
                ]
            )

            return current_player_season_game_logs

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

        positions = positions_df[positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()
        team_id = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['TeamID'].iloc[0]
        player_name = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]

        print(f"Finding points projection features for {player_name}...")

        average_last_3, average_last_5, average_last_7, average_last_10 = avg_last_3_5_7_10(season_game_logs, player_id, str(curr_date))
        minute_trend = points_trend_5(str(curr_date), player_id, player_name, season_game_logs)
        last_10_std_dev = find_last_10_std_dev(str(curr_date), player_id, player_name, season_game_logs)
        total_games_played_this_season = find_total_games_played_this_season(str(curr_date), player_id, season_game_logs)
        games_started_last_5 = find_games_started_last_5(str(curr_date), player_id, season_game_logs)
        position_missing_pts = find_position_missing_pts(conn, curr_date, positions, team_id)

        features_dict = {
            "AVERAGE_LAST_3": average_last_3, 
            "AVERAGE_LAST_5": average_last_5, 
            "AVERAGE_LAST_7": average_last_7, 
            "AVERAGE_LAST_10": average_last_10,
            "POSITION_MISSING_POINTS": position_missing_pts,
            "MINUTE_TREND": minute_trend, 
            "LAST_10_STANDARD_DEVIATION": last_10_std_dev,
            "TOTAL_GAMES_PLAYED_THIS_SEASON": total_games_played_this_season,
            "GAMES_STARTED_LAST_5": games_started_last_5,
        }


        return features_dict

    points_projection_features = pd.DataFrame([find_points_projection_features(
        conn=conn, 
        season_start_date=season_start_date, 
        curr_date=curr_date,
        season_game_logs=season_game_logs,
        curr_scoreboard=curr_scoreboard,
        positions_df=positions_df,
        player_id = player_id
    )])

    points_projection_model_path = os.path.join(config.XGBOOST_PATH, "points_projection_model.pkl")

    model = joblib.load(points_projection_model_path)

    points_projection = model.predict(points_projection_features)[0]

    return float(points_projection)


if __name__ == "__main__":

    config = load_config()

    con = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    curr_date = "2026-01-02"

    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ? AND GAME_DATE < ?", con, params=(config.SEASON_START_DATE, curr_date))
    scoreboard = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", con, params=(curr_date,))
    positions_df = pd.read_sql_query("SELECT * FROM PLAYER_POSITIONS")

    player_id = 203468 #cj mccollum




