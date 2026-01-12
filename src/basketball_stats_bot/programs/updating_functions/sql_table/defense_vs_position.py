import sqlite3
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

def update_defense_vs_position(conn, current_season_start_date):

    def update_table(curren_season_start_date, team_id, team_name, conn):

        cursor = conn.cursor()

        for position in ['PG', 'SG', 'SF', 'PF', 'C']:

            curr_position_vs = pd.read_sql_query("""

                SELECT g.*
                FROM player_game_logs g
                JOIN PLAYER_POSITIONS p
                    ON g.PLAYER_ID = p.PLAYER_ID
                WHERE g.GAME_DATE >= ?
                AND p.POSITION = ?
                AND g.OPPOSITION_ID = ?
                AND g.MIN > ?

            """, conn, params=(current_season_start_date, position, team_id, 0))
            
            games_played = len(curr_position_vs.drop_duplicates('GAME_ID'))
            total_minutes = curr_position_vs['MIN'].sum()
            points_per_48 = curr_position_vs['PTS'].sum() / total_minutes * 48
            rebounds_per_48 = curr_position_vs['REB'].sum() / total_minutes * 48
            assists_per_48 = curr_position_vs['AST'].sum() / total_minutes * 48
            threes_per_48 = curr_position_vs['FG3M'].sum() / total_minutes * 48
            blocks_per_48 = curr_position_vs['BLK'].sum() / total_minutes * 48
            steals_per_48 = curr_position_vs['STL'].sum() / total_minutes * 48
            points_rebounds_assists_per_48 = (curr_position_vs['PTS'].sum() + curr_position_vs['REB'].sum() + curr_position_vs['AST'].sum()) / total_minutes * 48
            points_rebounds_per_48 = (curr_position_vs['PTS'].sum() + curr_position_vs['REB'].sum()) / total_minutes * 48
            points_assists_per_48 = (curr_position_vs['PTS'].sum() + curr_position_vs['AST'].sum()) / total_minutes * 48
            rebounds_assists_per_48 = (curr_position_vs['REB'].sum() + curr_position_vs['AST'].sum()) / total_minutes * 48

            points_per_game = curr_position_vs['PTS'].sum() / games_played
            rebounds_per_game = curr_position_vs['REB'].sum() / games_played
            assists_per_game = curr_position_vs['AST'].sum() / games_played
            threes_per_game = curr_position_vs['FG3M'].sum() / games_played
            blocks_per_game = curr_position_vs['BLK'].sum() / games_played
            steals_per_game = curr_position_vs['STL'].sum() / games_played
            points_rebounds_assists_per_game = (curr_position_vs['PTS'].sum() + curr_position_vs['REB'].sum() + curr_position_vs['AST'].sum()) / games_played
            points_rebounds_per_game = (curr_position_vs['PTS'].sum() + curr_position_vs['REB'].sum()) / games_played
            points_assists_per_game = (curr_position_vs['PTS'].sum() + curr_position_vs['AST'].sum()) / games_played
            rebounds_assists_per_game = (curr_position_vs['REB'].sum() + curr_position_vs['AST'].sum()) / games_played


            current_date = datetime.now(ZoneInfo("America/New_York")).date()

            values_per_48 = [
                position,
                team_name,
                team_id,
                games_played,
                points_per_48,
                rebounds_per_48,
                assists_per_48,
                threes_per_48,
                blocks_per_48,
                steals_per_48,
                points_rebounds_assists_per_48,
                points_rebounds_per_48,
                points_assists_per_48,
                rebounds_assists_per_48,
                str(current_date)
            ]

            values_per_game = [
                position,
                team_name,
                team_id,
                games_played,
                points_per_game,
                rebounds_per_game,
                assists_per_game,
                threes_per_game,
                blocks_per_game,
                steals_per_game,
                points_rebounds_assists_per_game,
                points_rebounds_per_game,
                points_assists_per_game,
                rebounds_assists_per_game
            ]

            placeholders = ", ".join(['?']*len(values_per_48))
            cursor.execute(f"""

                INSERT OR REPLACE INTO DEFENSE_VS_POSITION_2025_2026 VALUES ({placeholders})

            """, values_per_48)

        conn.commit()

    team = pd.read_sql_query("SELECT * FROM DEFENSE_VS_POSITION_2025_2026", conn).drop_duplicates('TEAM_ID')

    today = datetime.now(ZoneInfo("America/New_York")).date()

    if team['LAST_UPDATED'].iloc[0] == str(today):

        print(f"Defense vs Position table was already been updated today ({today}).")
        return

    team_ids = team['TEAM_ID'].to_list()

    print(f"Updating Defense vs. Position Table...")

    for team_id in team_ids:
        
        team_name = team[team['TEAM_ID'] == team_id]['TEAM_NAME'].iloc[0]

        update_table(current_season_start_date, team_id, team_name, conn)
    
    print(f"Updated Defense vs Position Table")

if __name__ == '__main__':

    conn = sqlite3.connect(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")
    cursor = conn.cursor()

    current_season_start_date = '2025-10-21'

    update_defense_vs_position(conn, current_season_start_date)

    

