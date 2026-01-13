import sqlite3
import pandas as pd
import sys
import numpy as np
import json

from basketball_stats_bot.config import load_config

def update_combo_props_usage(conn):

    cursor = conn.cursor()

    cursor.execute("""

        UPDATE player_game_logs
            SET
                PCT_PRA_USAGE = (
                    SELECT
                        CASE
                            WHEN t.PRA = 0 THEN 0
                            ELSE (player_game_logs.PTS + player_game_logs.REB + player_game_logs.AST) * 1.0 / t.PRA
                        END
                    FROM TEAM_TOTALS_PER_PLAYER t
                    WHERE t.PLAYER_ID = player_game_logs.PLAYER_ID
                    AND t.GAME_ID = player_game_logs.GAME_ID
                ),

                PCT_PTS_REB_USAGE = (
                    SELECT
                        CASE
                            WHEN t.PTS_REB = 0 THEN 0
                            ELSE (player_game_logs.PTS + player_game_logs.REB) * 1.0 / t.PTS_REB
                        END
                    FROM TEAM_TOTALS_PER_PLAYER t
                    WHERE t.PLAYER_ID = player_game_logs.PLAYER_ID
                    AND t.GAME_ID = player_game_logs.GAME_ID
                ),

                PCT_PTS_AST_USAGE = (
                    SELECT
                        CASE
                            WHEN t.PTS_AST = 0 THEN 0
                            ELSE (player_game_logs.PTS + player_game_logs.AST) * 1.0 / t.PTS_AST
                        END
                    FROM TEAM_TOTALS_PER_PLAYER t
                    WHERE t.PLAYER_ID = player_game_logs.PLAYER_ID
                    AND t.GAME_ID = player_game_logs.GAME_ID
                ),

                PCT_REB_AST_USAGE = (
                    SELECT
                        CASE
                            WHEN t.REB_AST = 0 THEN 0
                            ELSE (player_game_logs.REB + player_game_logs.AST) * 1.0 / t.REB_AST
                        END
                    FROM TEAM_TOTALS_PER_PLAYER t
                    WHERE t.PLAYER_ID = player_game_logs.PLAYER_ID
                    AND t.GAME_ID = player_game_logs.GAME_ID
                )


    """)

    conn.commit()

def setting_nan_for_corrupted(conn, corrupted_game_ids):

    cursor = conn.cursor()

    for game_id in corrupted_game_ids:

        cursor.execute("""

        UPDATE player_game_logs
        SET
            PCT_PRA_USAGE = ?,
            PCT_PTS_REB_USAGE = ?,
            PCT_PTS_AST_USAGE = ?,
            PCT_REB_AST_USAGE = ?
        WHERE GAME_ID = ?

        """, (np.nan, np.nan, np.nan, np.nan, game_id))
    
    conn.commit()

def updating_team_totals_per_player_minutes(conn):

    cursor = conn.cursor()

    cursor.execute("""

    UPDATE TEAM_TOTALS_PER_PLAYER
    SET
        MIN = p.MIN
    FROM player_game_logs p
        WHERE p.GAME_ID = TEAM_TOTALS_PER_PLAYER.GAME_ID
        AND p.PLAYER_ID = TEAM_TOTALS_PER_PLAYER.PLAYER_ID

    """)

    conn.commit()

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()

    season_start_date = "2025-10-21"

    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ?", conn, params=(season_start_date,))

    team_totals_per_player_df = pd.read_sql_query("SELECT * FROM TEAM_TOTALS_PER_PLAYER", conn)

    season_game_ids = season_game_logs.drop_duplicates("GAME_ID")['GAME_ID'].to_list()

    with open(r"C:\Users\noahs\.vscode\basketball_stats_model\corrupted_GameRotation.json", "r") as f:

        corrupted_game_ids = json.load(f)
    
    updating_team_totals_per_player_minutes(conn=conn)
    
    print(f"Finished fixing all the pct shares")