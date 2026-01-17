from nba_api.stats.endpoints import commonteamroster, leaguegamefinder
from nba_api.live.nba.endpoints import scoreboard
import time
from datetime import datetime, timedelta
import json
import pandas as pd
import os
import sqlite3
from zoneinfo import ZoneInfo

from basketball_stats_bot.config import load_config

def update_scoreboard_to_team_roster(conn, current_season):

    def scoreboard_to_team_roster(current_season, curr_date, conn):

        config = load_config()

        def get_roster(home_team_id, teamTricode, opposition_id, opposition_tricode, current_season, venue, curr_game_id):

            roster = commonteamroster.CommonTeamRoster(
                team_id=home_team_id,
                season=current_season,
                timeout=30
            )

            df = roster.get_data_frames()[0]

            if venue == "home":

                df['MATCHUP'] = f"{teamTricode} vs. {opposition_tricode}"
            
            else:

                df['MATCHUP'] = f"{teamTricode} @ {opposition_tricode}"

            df['GAME_ID'] = curr_game_id
            df['date'] = curr_date
            df['team_tricode'] = teamTricode
            df['opposition_team_id'] = opposition_id
            df['opposition_tricode'] = opposition_tricode
            
            time.sleep(0.5)

            return df
        
        today = datetime.now(ZoneInfo(config.TIMEZONE)).date()

        if curr_date == str(today):

            board = scoreboard.ScoreBoard()

            if not board:

                print(f"No games on {curr_date}.")
                exit()

            games = board.games.get_dict()

            games_dict = {}
            gameIds = [game['gameId'] for game in games]

            for game in games:

                games_dict[game['gameId']] = {

                    'homeTeam': {
                        'teamName': f"{game['homeTeam']['teamCity']} {game['homeTeam']['teamName']}",
                        'teamId': game['homeTeam']['teamId'],
                        'teamTricode': game['homeTeam']['teamTricode'],
                    },

                    'awayTeam': {
                        'teamName': f"{game['awayTeam']['teamCity']} {game['awayTeam']['teamName']}",
                        'teamId': game['awayTeam']['teamId'],
                        'teamTricode': game['awayTeam']['teamTricode'],
                    }
                }

        else:

            gamefinder = leaguegamefinder.LeagueGameFinder(
                        date_from_nullable=curr_date,
                        date_to_nullable=curr_date
                    )

            games_df = gamefinder.get_data_frames()[0]
            gameIds = list(games_df['GAME_ID'].drop_duplicates())

            team_tricode_arr = games_df['TEAM_ABBREVIATION'].to_list()
            team_id_arr = games_df['TEAM_ID'].to_list()
            matchup_arr = games_df['MATCHUP'].to_list()
            game_id_arr = games_df['GAME_ID'].to_list()
            team_name_arr = games_df['TEAM_NAME'].to_list()

            games_dict = {}

            for i in range(len(game_id_arr)):

                if game_id_arr[i] in games_dict:

                    curr = games_dict[game_id_arr[i]]

                    if not curr['homeTeam']['teamId']:
                        
                        curr['homeTeam']['teamName'] = team_name_arr[i]
                        curr['homeTeam']['teamId'] = team_id_arr[i]
                        curr['homeTeam']['teamTricode'] = team_tricode_arr[i]
                    
                    else:
                        
                        curr['awayTeam']['teamName'] = team_name_arr[i]
                        curr['awayTeam']['teamId'] = team_id_arr[i]
                        curr['awayTeam']['teamTricode'] = team_tricode_arr[i]
                    
                else:

                    games_dict[game_id_arr[i]] = {

                        'homeTeam': {
                            'teamName': None,
                            'teamId': None,
                            'teamTricode': None
                        }, 
                        'awayTeam': {
                            'teamName': None,
                            'teamId': None,
                            'teamTricode': None
                        }

                    }

                    curr = games_dict[game_id_arr[i]]

                    if '@' in matchup_arr[i]:

                        curr['awayTeam']['teamName'] = team_name_arr[i]
                        curr['awayTeam']['teamId'] = team_id_arr[i]
                        curr['awayTeam']['teamTricode'] = team_tricode_arr[i]
                    
                    else:

                        curr['homeTeam']['teamName'] = team_name_arr[i]
                        curr['homeTeam']['teamId'] = team_id_arr[i]
                        curr['homeTeam']['teamTricode'] = team_tricode_arr[i]

        today_nba_api_gameIds = []

        for game in gameIds:
            
            today_nba_api_gameIds.append(game)
        

        dir_path = os.path.join(config.GAME_FILES_PATH, curr_date)

        if not os.path.isdir(dir_path):

            os.mkdir(dir_path)

        file_path = os.path.join(config.GAME_FILES_PATH, curr_date, "nba_api_game_ids.json")

        with open(file_path, "w") as f:

            json.dump(today_nba_api_gameIds, f, indent=4)

        dfs = []

        print(f"\nFinding rosters for {curr_date}...\n")
        
        for curr_game_id, game in games_dict.items():

            home_team = game['homeTeam']
            away_team = game['awayTeam']

            print(f"Extracting roster from the {home_team['teamName']}\n")
            dfs.append(get_roster(home_team['teamId'], home_team['teamTricode'], away_team['teamId'], away_team['teamTricode'], current_season, "home", curr_game_id))

            print(f"Extracting roster from the {away_team['teamName']}\n")
            dfs.append(get_roster(away_team['teamId'], away_team['teamTricode'], home_team['teamId'], home_team['teamTricode'], current_season, "away", curr_game_id))
        
        scoreboard_to_team_roster_df = pd.concat(dfs, ignore_index=True)

        file_path = os.path.join(config.GAME_FILES_PATH, curr_date, "scoreboard_to_team_roster_output.json")

        scoreboard_to_team_roster_df.to_json(file_path, orient="records", indent=4)

        cursor = conn.cursor()

        row = list(scoreboard_to_team_roster_df)
        row_names = ", ".join(row)
        placeholders = ", ".join(['?'] * len(row))

        for _, row in scoreboard_to_team_roster_df.iterrows():

            
            cursor.execute(f"""

                INSERT OR REPLACE INTO SCOREBOARD_TO_ROSTER ({row_names})
                VALUES ({placeholders})

            """, row.to_list())

            conn.commit()

    latest_date_str = "2024-10-24"
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime("2025-06-22", "%Y-%m-%d").date()

    while curr_date <= end_date:

        check_for_existing_df = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(str(curr_date),))

        if not check_for_existing_df.empty:

            print(f"Already found a scoreboard_to_roster for {curr_date}")
            curr_date += timedelta(days=1)
            continue
        
        scoreboard_to_team_roster(current_season=current_season, curr_date=str(curr_date), conn=conn)

        curr_date += timedelta(days=1)
    
    print(f"Finished updating SCOREBOARD_TO_ROSTER SQL Table from {latest_date_str} - {end_date}")


if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    current_season = "2024-25"

    update_scoreboard_to_team_roster(conn=conn, current_season=current_season)