import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import joblib
import sys
import numpy as np

from xgboost import plot_importance
import xgboost as xgb

from nba_api.live.nba.endpoints import boxscore

from basketball_stats_bot.config import load_config

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)
    cur = conn.cursor()

    cur.execute("""

        CREATE TABLE PTS_PROJECTION
        VALUES = (
                GAME_DATE DATE,
                GAME_ID TEXT,
                PLAYER_ID INT,
                PLAYER_NAME TEXT,
                AVERAGE_LAST_3 FLOAT,
                AVERAGE_LAST_5 FLOAT,
                AVERAGE_LAST_7 FLOAT,
                AVERAGE_LAST_10 FLOAT,
                POINTS_TREND FLOAT,
                POSITION_MISSING_POINTS FLOAT,
                LAST_10_STANDARD_DEVIATION FLOAT,
                STARTER INT,
                POINTS INT
        )

    """)
    
    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ? AND MIN > 0 ORDER BY GAME_DATE DESC", conn, params=(config.SEASON_START_DATE,))

    dnps = pd.read_sql_query("SELECT * FROM DNPS", conn)

    game_dates = dnps['GAME_DATE'].to_list()
    player_ids = dnps['PLAYER_ID'].to_list()
    player_names = dnps['PLAYER_NAME'].to_list()

    dates_pids_names = list(zip(game_dates, player_ids, player_names))

    for i in range(len(dates_pids_names)):

        date, pid, player_name = dates_pids_names[i]

        print(f"Updating average points for {player_name} on {date}.. ({i+1}/{len(dates_pids_names)})")

        curr_game_logs = season_game_logs[
            (season_game_logs['GAME_DATE'] < date) &
            (season_game_logs['PLAYER_ID'] == pid)
        ]

        print(curr_game_logs)
        sys.exit(1)

        if curr_game_logs.empty:

            average_points = np.nan
        
        else:

            average_points = curr_game_logs['PTS'].sum() / len(curr_game_logs)
        
        cur.execute("""

            UPDATE DNPS
                    SET 
                        AVERAGE_PTS = ?
                    WHERE GAME_DATE = ?
                    AND PLAYER_ID = ?

        """, (average_points, date, pid))

    conn.commit()






    