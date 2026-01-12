from nba_api.stats.endpoints import BoxScorePlayerTrackV3, boxscoreadvancedv3
from nba_api.live.nba.endpoints import boxscore
from nba_api.live.nba.endpoints import ScoreBoard
import pandas as pd
import json
import sqlite3
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

def update_dnps_table(conn, season_start_date):

    def find_average_minutes(player_id, curr_date, game_logs):

        player_game_logs_before_curr_date = game_logs[
            (game_logs['GAME_DATE'] < curr_date) &
            (game_logs['MIN'] > 0) &
            (game_logs['PLAYER_ID'] == player_id)
        ]

        if len(player_game_logs_before_curr_date) == 0:

            average_minutes = 0
        
        else:

            average_minutes = float(player_game_logs_before_curr_date['MIN'].sum()) / len(player_game_logs_before_curr_date)

        return average_minutes
    
    cursor = conn.cursor()
    
    check_for_last_date_updated = pd.read_sql_query("SELECT * FROM DNPS ORDER BY GAME_DATE DESC", conn)

    latest_date_str = check_for_last_date_updated['GAME_DATE'].iloc[0]
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
    today = datetime.now(ZoneInfo("America/New_York")).date()

    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ?", conn, params=(season_start_date,))

    while curr_date < today:

        print(f"Updating DNPS sql table for {curr_date}..")

        curr_date_game_ids = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(str(curr_date),)).drop_duplicates("GAME_ID")['GAME_ID'].to_list()

        for game_id in curr_date_game_ids:

            box = boxscore.BoxScore(game_id=game_id).get_dict()

            homeTeam = box['game']['homeTeam']
            awayTeam = box['game']['awayTeam']

            for player in homeTeam['players']:

                if player['status'] != "ACTIVE":

                    team_id = homeTeam['teamId']
                    team_name = f"{homeTeam['teamCity']} {homeTeam['teamName']}"
                    player_name = player['name']
                    player_id = player['personId']
                    curr_avg_min = find_average_minutes(player_id, str(curr_date), season_game_logs)

                    stats = [str(curr_date), game_id, team_id, team_name, player_id, player_name, curr_avg_min]

                    placeholders = ", ".join(['?']*len(stats))

                    cursor.execute(f"INSERT OR REPLACE INTO DNPS VALUES ({placeholders})", stats)

        curr_date += timedelta(days=1)

    conn.commit()
    print(f"Finished updating the DNPS sql table from {latest_date_str} - {curr_date}")

if __name__ == "__main__":

    conn = sqlite3.connect(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")
    season_start_date = "2025-10-21"

    update_dnps_table(conn, season_start_date)








