import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import sys
import time

from  basketball_stats_bot.config import load_config

def find_overall_averages(curr_game_logs, curr_date, stat, prop_line):

    games_before_curr_date = curr_game_logs[curr_game_logs['GAME_DATE'] < str(curr_date)]

    games_before_curr_date = games_before_curr_date.sort_values("GAME_DATE", ascending=True)

    if games_before_curr_date.empty:

        print(f"Could not find gameloogs for current player before {curr_date}")
        return np.nan, np.nan, np.nan, np.nan, 9

    games_played = len(games_before_curr_date)

    average_L3_minus_line = games_before_curr_date.iloc[:3][stat].sum() / len(games_before_curr_date.iloc[:3][stat]) - prop_line
    average_L5_minus_line = games_before_curr_date.iloc[:5][stat].sum() / len(games_before_curr_date.iloc[:5][stat]) - prop_line
    average_L7_minus_line = games_before_curr_date.iloc[:7][stat].sum() / len(games_before_curr_date.iloc[:7][stat]) - prop_line
    average_L10_minus_line = games_before_curr_date.iloc[:10][stat].sum() / len(games_before_curr_date.iloc[:10][stat]) - prop_line

    return average_L3_minus_line, average_L5_minus_line, average_L7_minus_line, average_L10_minus_line, games_played

def find_opp_averages(curr_game_logs, curr_date, stat, prop_line):

    games_before_curr_date = curr_game_logs[curr_game_logs['GAME_DATE'] < str(curr_date)]

    games_before_curr_date = games_before_curr_date.sort_values("GAME_DATE", ascending=True)

    if games_before_curr_date.empty:

        print(f"Could not find gameloogs for current player before {curr_date}")
        return np.nan, np.nan, 0

    games_played = len(games_before_curr_date)

    average_L3_minus_line = games_before_curr_date.iloc[:3][stat].sum() / len(games_before_curr_date.iloc[:3][stat]) - prop_line
    average_L7_minus_line = games_before_curr_date.iloc[:7][stat].sum() / len(games_before_curr_date.iloc[:7][stat]) - prop_line

    return average_L3_minus_line, average_L7_minus_line, games_played

def find_ema_averages(curr_game_logs, prop):

    for span in [3, 5, 7, 10, 20]:

        curr_game_logs.loc[:, f"EMA_{span}_{prop}"] = (
            curr_game_logs
            .groupby("PLAYER_ID")[prop]
            .ewm(span=span, adjust=False)
            .mean()
            .shift(1)
            .reset_index(level=0, drop=True)
        )

    return curr_game_logs

if __name__ == "__main__":

    start = time.time()

    config = load_config()

    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()

    season_start_date = "2025-10-21"
    curr_date_str = "2025-10-21"
    end_date_str = "2026-01-09"
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ? AND MIN > 0", conn, params=(season_start_date,))
    game_logs = pd.read_sql_query("SELECT * FROM player_game_logs", conn)
    scoreboard = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date >= ?", conn, params=(season_start_date,))
    player_props = pd.read_sql_query("SELECT * FROM PLAYER_PROPS", conn)

    props = [
        'PTS',
        'REB',
        'AST',
        'FG3M',
        'PRA',
        'PTS_REB',
        'PTS_AST',
        'REB_AST'
    ]

    season_game_logs = season_game_logs.sort_values(["PLAYER_ID", "GAME_DATE"])

    # for prop in props:

    #     cursor.execute(f"""

    #         UPDATE PROPS_TRAINING_TABLE AS t
    #         SET 
    #             EMA_LAST_3_OVERALL = e.EMA_3_{prop} - t.PROP_LINE,
    #             EMA_LAST_5_OVERALL = e.EMA_5_{prop} - t.PROP_LINE,
    #             EMA_LAST_7_OVERALL = e.EMA_7_{prop} - t.PROP_LINE,
    #             EMA_LAST_10_OVERALL = e.EMA_10_{prop} - t.PROP_LINE,
    #             EMA_LAST_20_OVERALL = e.EMA_20_{prop} - t.PROP_LINE
    #         FROM ema_temp e
    #         WHERE t.PROP = ?
    #         AND t.GAME_ID = e.GAME_ID
    #         AND t.PLAYER_ID = e.PLAYER_ID

    #     """, (prop,))

    # conn.commit()
    # sys.exit(1)      

    season_game_logs.to_sql(
        "ema_temp",
        conn,
        if_exists="replace",
        index=False
    )
    sys.exit(1)
    
    conn.commit()
    end = time.time()
    
    print(f"Finished finding ema averages for PROPS_TRAINING_TABLE from {curr_date_str} - {end_date_str}. Elapsed: {end - start:.2f}")




