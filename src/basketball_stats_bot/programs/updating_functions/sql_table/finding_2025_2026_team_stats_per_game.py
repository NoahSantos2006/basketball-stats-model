import sqlite3
import pandas as pd
from nba_api.live.nba.endpoints import boxscore
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

def update_team_stats(conn):

    cursor = conn.cursor()

    team_stats = pd.read_sql_query("SELECT * FROM TEAM_STATS_2025_2026 WHERE GAME_DATE ORDER BY GAME_DATE DESC LIMIT 1", conn)
    nba_api_game_ids = pd.read_sql_query("SELECT * FROM NBA_API_GAME_IDS", conn)
    latest_date_str = team_stats['GAME_DATE'].iloc[0]
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
    end_date = datetime.now(ZoneInfo("America/New_York")).date()

    while curr_date < end_date:

        print(f"Updating TEAM_STATS_2025_2026 SQL Table for {str(curr_date)}..")

        curr_game_ids = nba_api_game_ids[nba_api_game_ids['DATE'] == str(curr_date)]['GAME_ID'].to_list()

        for game_id in curr_game_ids:

            box = boxscore.BoxScore(game_id=game_id).get_dict()

            homeTeam = box['game']['homeTeam']
            home_team_name = f"{homeTeam['teamCity']} {homeTeam['teamName']}"
            awayTeam = box['game']['awayTeam']
            away_team_name = f"{awayTeam['teamCity']} {awayTeam['teamName']}"
            game_date = box['game']['gameEt'][:10]
            
            print(f"Finding team stats for {home_team_name} vs. {away_team_name}...")

            home_team_statistics = homeTeam['statistics']

            homeTeam_stats = [
                game_date,
                game_id,
                homeTeam['teamId'],
                home_team_name,
                home_team_statistics['assists'],
                home_team_statistics['benchPoints'],
                home_team_statistics['blocks'],
                home_team_statistics['fieldGoalsAttempted'],
                home_team_statistics['fieldGoalsMade'],
                home_team_statistics['fieldGoalsPercentage']*100,
                home_team_statistics['foulsDrawn'],
                home_team_statistics['foulsPersonal'],
                home_team_statistics['freeThrowsAttempted'],
                home_team_statistics['freeThrowsMade'],
                home_team_statistics['freeThrowsPercentage']*100,
                home_team_statistics['points'],
                home_team_statistics['pointsAgainst'],
                home_team_statistics['reboundsTotal'],
                home_team_statistics['steals'],
                home_team_statistics['threePointersAttempted'],
                home_team_statistics['threePointersMade'],
                home_team_statistics['threePointersPercentage']*100,
                home_team_statistics['trueShootingAttempts'],
                home_team_statistics['trueShootingPercentage']*100,
                home_team_statistics['turnovers'],
                home_team_statistics['twoPointersAttempted'],
                home_team_statistics['twoPointersMade'],
                home_team_statistics['twoPointersPercentage']*100

            ]

            away_team_statistics = awayTeam['statistics']

            awayTeam_stats = [
                game_date,
                game_id,
                awayTeam['teamId'],
                away_team_name,
                away_team_statistics['assists'],
                away_team_statistics['benchPoints'],
                away_team_statistics['blocks'],
                away_team_statistics['fieldGoalsAttempted'],
                away_team_statistics['fieldGoalsMade'],
                away_team_statistics['fieldGoalsPercentage']*100,
                away_team_statistics['foulsDrawn'],
                away_team_statistics['foulsPersonal'],
                away_team_statistics['freeThrowsAttempted'],
                away_team_statistics['freeThrowsMade'],
                away_team_statistics['freeThrowsPercentage']*100,
                away_team_statistics['points'],
                away_team_statistics['pointsAgainst'],
                away_team_statistics['reboundsTotal'],
                away_team_statistics['steals'],
                away_team_statistics['threePointersAttempted'],
                away_team_statistics['threePointersMade'],
                away_team_statistics['threePointersPercentage']*100,
                away_team_statistics['trueShootingAttempts'],
                away_team_statistics['trueShootingPercentage']*100,
                away_team_statistics['turnovers'],
                away_team_statistics['twoPointersAttempted'],
                away_team_statistics['twoPointersMade'],
                away_team_statistics['twoPointersPercentage']*100

            ]
            
            placeholders = ", ".join(['?']*len(homeTeam_stats))

            cursor.execute(f"INSERT OR REPLACE INTO TEAM_STATS_2025_2026 VALUES ({placeholders})", homeTeam_stats)
            cursor.execute(f"INSERT OR REPLACE INTO TEAM_STATS_2025_2026 VALUES ({placeholders})", awayTeam_stats)
        
        curr_date += timedelta(days=1)

    conn.commit()
    
if __name__ == "__main__":


    conn = sqlite3.connect(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")
    
    update_team_stats(conn)
        
    conn.commit()

