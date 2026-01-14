import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import sys

from basketball_stats_bot.config import load_config

def find_position_missing_stats(conn, curr_date, positions, team_id, stat):

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

    total_pos_stat = cat.drop_duplicates('PLAYER_ID')[f'AVERAGE_{stat}'].sum()
    
    return total_pos_stat

if __name__ == "__main__":

    config = load_config()

    con = sqlite3.connect(config.DB_ONE_DRIVE_PATH)
    cur = con.cursor()

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

    props_training_table = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE", con)
    dnps = pd.read_sql_query("SELECT * FROM DNPS", con)
    positions_df = pd.read_sql_query("SELECT * FROM PLAYER_POSITIONS", con)
    scoreboard = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER", con)

    curr_date = datetime.strptime("2025-11-20", "%Y-%m-%d").date()
    end_date = datetime.strptime("2026-01-12", "%Y-%m-%d").date()

    while curr_date <= end_date:

        curr_props_table = props_training_table[props_training_table['GAME_DATE'] == str(curr_date)]
        curr_scoreboard = scoreboard[scoreboard['date'] == str(curr_date)]

        if curr_props_table.empty:

            curr_date += timedelta(days=1)
            continue

        curr_dnps = dnps[dnps['GAME_DATE'] == str(curr_date)]
        
        player_ids = curr_props_table.drop_duplicates("PLAYER_ID")['PLAYER_ID'].to_list()

        for pid in player_ids:

            print(f"Updating {pid} on {curr_date}...")

            curr_positions = positions_df[positions_df['PLAYER_ID'] == pid]
            curr_player_props_table = curr_props_table[curr_props_table['PLAYER_ID'] == pid]
            player_scoreboard = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == pid]

            if player_scoreboard.empty:

                print(f"Could not find a scoreboard for {pid} on {curr_date}.")
                continue
                
            curr_tid = player_scoreboard['TeamID'].iloc[0]

            if curr_positions.empty:

                print(f"Could not find positions for {pid}")
                sys.exit(1)
            
            curr_positions = curr_positions['POSITION'].to_list()
            curr_props = curr_player_props_table['PROP'].to_list()

            for prop in curr_props:

                if prop in {'BLK', 'STL'}:

                    continue
                
                position_missing_stat = find_position_missing_stats(con, str(curr_date), curr_positions, int(curr_tid), prop)

                cur.execute("""

                UPDATE PROPS_TRAINING_TABLE
                SET 
                    POSITION_MISSING_STAT = ?
                WHERE PROP = ?
                AND GAME_DATE = ?
                AND PLAYER_ID = ?
                            
                """, (position_missing_stat, prop, str(curr_date), pid))
        
        curr_date += timedelta(days=1)
    
    con.commit()
        
        

