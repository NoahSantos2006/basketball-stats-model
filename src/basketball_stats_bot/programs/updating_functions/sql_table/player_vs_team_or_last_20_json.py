import sqlite3
from datetime import datetime, timedelta
import os
import json
import pandas as pd
import sys

from basketball_stats_bot.config import load_config

def player_vs_team_or_last_20(scoreboard_to_team_roster_df, date, season_start_date, conn):
    
    def player_game_logs_df(player_name, player_id, opposition_team_id, curr_date):

            print(f"Finding gamelogs for {player_name}...")
            
            player_vs_team_df = pd.read_sql_query(f"""
                                   
                            SELECT * FROM player_game_logs 
                            WHERE PLAYER_ID = ?
                            AND OPPOSITION_ID = ?
                            AND GAME_DATE < ?
                            AND MIN > ?
                            ORDER BY GAME_DATE DESC
                            LIMIT 10
                           
                           """, conn, params=(player_id, opposition_team_id, curr_date, 0))
            
            last_20_games_df = pd.read_sql_query(f"""
                           
                           SELECT * FROM player_game_logs 
                           WHERE PLAYER_ID = ?
                           AND OPPOSITION_ID != ?
                           AND SEASON_ID > {season_start_date}
                           AND GAME_DATE < ?
                           AND MIN > ?
                           ORDER BY GAME_DATE DESC
                           LIMIT 20
                           
                           """, conn, params=(player_id, opposition_team_id, curr_date, 0))

            for cell in ['FT_PCT', 'FG_PCT', 'FG3_PCT']:
                 
                 player_vs_team_df[cell] = player_vs_team_df[cell].astype('float64')
                 last_20_games_df[cell] = last_20_games_df[cell].astype('float64')

            player_vs_team_and_last_20_df = pd.concat([player_vs_team_df, last_20_games_df], ignore_index=True)

            return player_vs_team_and_last_20_df
        
    print(f"\nFinding player game logs for {date}\n")

    player_names = scoreboard_to_team_roster_df['PLAYER'].to_list()
    player_ids = scoreboard_to_team_roster_df["PLAYER_ID"].tolist()
    opposition_team_id = scoreboard_to_team_roster_df["opposition_team_id"].tolist()

    dfs = []

    if len(player_names) > 0:

        for player_name, player_id, opp_id in list(zip(player_names, player_ids, opposition_team_id)):
            
            curr_df = player_game_logs_df(player_name, player_id, opp_id, curr_date)

            if not curr_df.empty:
                dfs.append(curr_df)
            
        player_vs_team_or_last_20_df = pd.concat(dfs, ignore_index=True)

        return player_vs_team_or_last_20_df
    
    return pd.DataFrame()

if __name__ == "__main__":

    config = load_config()

    curr_date_str = "2025-12-25"
    end_date_str = "2025-12-16"
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()
    
    while curr_date <= end_date:

        current_season_id_finder_df = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE = ? LIMIT 1", conn, params=(str(curr_date),))

        if current_season_id_finder_df.empty:
             
             curr_date += timedelta(days=1)
             continue
        
        season_start_date = '2025-10-21'

        scoreboard_to_team_roster_df = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE DATE = ?", conn, params=(str(curr_date),))

        json_file = player_vs_team_or_last_20(scoreboard_to_team_roster_df, str(curr_date), season_start_date, conn)

        if json_file.empty:
             
             curr_date += timedelta(days=1)
             continue

        cursor.execute("""

            INSERT OR REPLACE INTO PLAYER_VS_TEAM_OR_LAST_20_JSONS (DATE, JSON_FILE)
            VALUES (?, ?)

            """, (str(curr_date), json_file.to_json()))

        conn.commit()

        curr_date += timedelta(days=1)
    
    print(f"json files updated")

