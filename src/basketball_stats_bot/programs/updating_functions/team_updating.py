from datetime import datetime, timedelta
import pandas as pd
from zoneinfo import ZoneInfo
from nba_api.live.nba.endpoints import boxscore


def update_defense_vs_position(conn, current_season_start_date):

    def update_table(current_season_start_date, team_id, team_name, conn):

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

    team_ids = team.drop_duplicates('TEAM_ID')['TEAM_ID'].to_list()

    print(f"Updating Defense vs. Position Table...")

    for team_id in team_ids:
        
        team_name = team[team['TEAM_ID'] == team_id]['TEAM_NAME'].iloc[0]

        update_table(current_season_start_date, team_id, team_name, conn)
    
    print(f"Updated Defense vs Position Table")

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
                home_team_statistics['twoPointersPercentage']*100,
                home_team_statistics['points'] + home_team_statistics['assists'] + home_team_statistics['reboundsTotal'],
                home_team_statistics['points'] + home_team_statistics['reboundsTotal'],
                home_team_statistics['points'] + home_team_statistics['assists'],
                home_team_statistics['reboundsTotal'] + home_team_statistics['assists'] 
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
                away_team_statistics['twoPointersPercentage']*100,
                away_team_statistics['points'] + away_team_statistics['assists'] + away_team_statistics['reboundsTotal'],
                away_team_statistics['points'] + away_team_statistics['reboundsTotal'],
                away_team_statistics['points'] + away_team_statistics['assists'],
                away_team_statistics['reboundsTotal'] + away_team_statistics['assists'] 

            ]
            
            placeholders = ", ".join(['?']*len(homeTeam_stats))

            cursor.execute(f"INSERT OR REPLACE INTO TEAM_STATS_2025_2026 VALUES ({placeholders})", homeTeam_stats)
            cursor.execute(f"INSERT OR REPLACE INTO TEAM_STATS_2025_2026 VALUES ({placeholders})", awayTeam_stats)
        
        curr_date += timedelta(days=1)

    conn.commit()


