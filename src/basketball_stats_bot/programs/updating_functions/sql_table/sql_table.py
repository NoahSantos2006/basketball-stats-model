import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from nba_api.live.nba.endpoints import boxscore


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



