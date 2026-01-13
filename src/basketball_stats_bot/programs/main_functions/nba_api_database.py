from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import commonteamroster, leaguegamefinder
import time
import os
import json
import pandas as pd
import sys
from datetime import datetime, timedelta
import sqlite3
from zoneinfo import ZoneInfo
from io import StringIO

from basketball_stats_bot.config import load_config

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

    if str(curr_date) == str(today):

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
                    date_from_nullable=str(curr_date),
                    date_to_nullable=str(curr_date)
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
    

    dir_path = os.path.join(config.GAME_FILES_PATH, str(curr_date))

    if not os.path.isdir(dir_path):

        os.mkdir(dir_path)

    file_path = os.path.join(config.GAME_FILES_PATH, str(curr_date), "nba_api_game_ids.json")

    with open(file_path, "w") as f:

        json.dump(today_nba_api_gameIds, f, indent=4)

    dfs = []

    check_for_existing_df = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(str(curr_date),))

    if not check_for_existing_df.empty:

        print(f"Already found a scoreboard_to_roster for {curr_date}")
        return check_for_existing_df

    print(f"\nFinding rosters for {curr_date}...\n")
    
    for curr_game_id, game in games_dict.items():

        home_team = game['homeTeam']
        away_team = game['awayTeam']

        print(f"Extracting roster from the {home_team['teamName']}\n")
        dfs.append(get_roster(home_team['teamId'], home_team['teamTricode'], away_team['teamId'], away_team['teamTricode'], current_season, "home", curr_game_id))

        print(f"Extracting roster from the {away_team['teamName']}\n")
        dfs.append(get_roster(away_team['teamId'], away_team['teamTricode'], home_team['teamId'], home_team['teamTricode'], current_season, "away", curr_game_id))
    
    scoreboard_to_team_roster_df = pd.concat(dfs, ignore_index=True)

    file_path = os.path.join(config.GAME_FILES_PATH, str(curr_date), "scoreboard_to_team_roster_output.json")

    scoreboard_to_team_roster_df.to_json(file_path, orient="records", indent=4)

    scoreboard_to_team_roster_df.to_sql(

        name="SCOREBOARD_TO_ROSTER",
        con=conn,
        if_exists="append",
        index=False

    )

    return scoreboard_to_team_roster_df

def player_vs_team_or_last_20(scoreboard_to_team_roster_df, date, current_season_start_date, conn):

    config = load_config()
    
    def find_player_game_logs_df(all_player_game_logs, player_name, player_id, opposition_team_id, curr_date):

            print(f"Finding gamelogs for {player_name}...")

            player_vs_team_df = all_player_game_logs[
                (all_player_game_logs['PLAYER_ID'] == player_id) &
                (all_player_game_logs['OPPOSITION_ID'] == opposition_team_id) &
                (all_player_game_logs['GAME_DATE'] < curr_date) &
                (all_player_game_logs['MIN'] > 0)
            ]

            last_20_games_df = all_player_game_logs[
                (all_player_game_logs['PLAYER_ID'] == player_id) &
                (all_player_game_logs['OPPOSITION_ID'] != opposition_team_id) &
                (all_player_game_logs['GAME_DATE'] >= current_season_start_date) &
                (all_player_game_logs['GAME_DATE'] < curr_date) &
                (all_player_game_logs['MIN'] > 0)
            ]

            player_vs_team_df = player_vs_team_df = player_vs_team_df.sort_values("GAME_DATE", ascending=False).iloc[:10]
            last_20_games_df = last_20_games_df.sort_values("GAME_DATE", ascending=False).iloc[:20]

            for cell in ['FT_PCT', 'FG_PCT', 'FG3_PCT']:
                 
                 player_vs_team_df[cell] = player_vs_team_df[cell].astype('float64')
                 last_20_games_df[cell] = last_20_games_df[cell].astype('float64')

            player_vs_team_and_last_20_df = pd.concat([player_vs_team_df, last_20_games_df], ignore_index=True)

            player_vs_team_and_last_20_df = player_vs_team_and_last_20_df.sort_values('GAME_DATE', ascending=False).reset_index(drop=True)

            return player_vs_team_and_last_20_df

    cursor = conn.cursor()

    check_for_existing = pd.read_sql_query("SELECT * FROM PLAYER_VS_TEAM_OR_LAST_20_JSONS WHERE DATE = ?", conn, params=(str(date),))

    if not check_for_existing.empty:

        df = pd.read_json(StringIO(check_for_existing['JSON_FILE'].iloc[0]))

        return df

    print(f"\nFinding player game logs for {date}\n")

    player_names = scoreboard_to_team_roster_df['PLAYER'].to_list()
    player_ids = scoreboard_to_team_roster_df["PLAYER_ID"].tolist()
    opposition_team_ids = scoreboard_to_team_roster_df["opposition_team_id"].tolist()
    all_player_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs", conn)

    dfs = []

    for player_name, player_id, opp_id in list(zip(player_names, player_ids, opposition_team_ids)):
        
        curr_df = find_player_game_logs_df(all_player_game_logs, player_name, player_id, opp_id, str(date))

        if not curr_df.empty:
            dfs.append(curr_df)
        
    player_vs_team_or_last_20_df = pd.concat(dfs, ignore_index=True)

    player_vs_team_or_last_20_path = os.path.join(config.GAME_FILES_PATH, str(date), 'player_vs_team_or_last_20.json')

    player_vs_team_or_last_20_df.to_json(player_vs_team_or_last_20_path, orient='records', indent=4)

    cursor.execute("INSERT OR REPLACE INTO PLAYER_VS_TEAM_OR_LAST_20_JSONS VALUES(?, ?)", (str(date), player_vs_team_or_last_20_df.to_json()))
    conn.commit()

    return player_vs_team_or_last_20_df

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    season_start_date = "2025-10-21"
    current_season = "2025-26"
    curr_date_str = "2026-01-02"
    end_date_str = "2026-01-02"

    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    config = load_config()

    scoreboard_to_team_roster_df = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(curr_date_str,))


    while curr_date <= end_date:

        player_vs_team_or_last_20(
            scoreboard_to_team_roster_df=scoreboard_to_team_roster_df,
            date=curr_date,
            current_season_start_date=season_start_date,
            conn=conn
        )

        curr_date += timedelta(days=1)

    