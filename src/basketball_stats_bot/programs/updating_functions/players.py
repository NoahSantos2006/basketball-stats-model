import pandas as pd
from datetime import datetime, timedelta
import unicodedata
from zoneinfo import ZoneInfo
import isodate
import json
import os
import time
import sys
import sqlite3
from requests import Timeout
import numpy as np

from nbainjuries import injury
import nbainjuries.injury as injury_mod

from basketball_stats_bot.config import load_config

from nba_api.stats.endpoints import boxscoreusagev3, leaguegamefinder, commonteamroster, playbyplayv3, GameRotation
from nba_api.live.nba.endpoints import boxscore, scoreboard


def update_dnps_from_nbainjuries(conn, season_start_date, curr_date):

    user_input = input(f"Do you want to find injuries from NBAINJURIES API for {curr_date}? (y/n): ").lower()

    while user_input not in {'y', 'n'}:

        user_input = input(f"Do you want to find injuries from NBAINJURIES API for {curr_date}? (y/n): ").lower()

    if user_input == "n":
        return 
    print(f"Finding injuries from NBAINJURIES API on {curr_date}...")

    # changes timestamp because nbainjuries api isn't fully updated
    def _gen_url_fixed(timestamp):
        URLstem_date = timestamp.date().strftime('%Y-%m-%d')
        URLstem_time = timestamp.strftime('%I_%M%p')

        return injury_mod._constants.urlstem_injreppdf.replace(
            '*',
            URLstem_date + '_' + URLstem_time
        )

    def _gen_filepath_fixed(timestamp, directorypath):
        URLstem_date = timestamp.date().strftime('%Y-%m-%d')
        URLstem_time = timestamp.strftime('%I_%M%p')
        filename = f'Injury-Report_{URLstem_date}_{URLstem_time}.pdf'
        return injury_mod.path.join(directorypath, filename)

    injury_mod._gen_url = _gen_url_fixed
    injury_mod._gen_filepath = _gen_filepath_fixed

    def find_today_injuries(conn, curr_date):

        def clean_name(text):

            removed_accents_text =  "".join(
                c for c in unicodedata.normalize('NFD', text)
                if unicodedata.category(c) != "Mn"
            )

            clean = removed_accents_text.replace(".", "")

            return clean

        def parse_nba_injuries(injury_df, player_game_logs_df):

            name_edge_cases = {
                'Hansen Yang': 'Yang Hansen',
                'Nikola Djurisic': 'Nikola Đurisic',
            }

            player_names_list_from_df = injury_df.dropna()['Player Name'].to_list()

            player_names_list = []

            for player in player_names_list_from_df:

                last_first = player.split(',')
                first_last = f"{last_first[1].strip()} {last_first[0].strip()}"
                player_names_list.append(first_last)

            status_list = injury_df.dropna()['Current Status'].to_list()

            player_status_list = list(zip(player_names_list, status_list))

            player_status_dicts = []

            for player, status in player_status_list:

                if player_game_logs_df[player_game_logs_df['NAME_CLEAN'] == clean_name(player)].empty:

                    if player not in name_edge_cases:
                        
                        print(f"Could not find game logs for {player}. Check parse_nba_injuries function..")
                        
                    else: 

                        player = name_edge_cases[player]

                player_game_logs = player_game_logs_df[player_game_logs_df['NAME_CLEAN'] == clean_name(player)]

                if player_game_logs.empty:

                    print(f"Could not find game logs for {player}. Check parse_nba_injuries function..")
                    continue

                player_id = int(player_game_logs['PLAYER_ID'].iloc[0])

                player_status_dicts.append({

                    'PLAYER_NAME': player,
                    'NAME_CLEAN': clean_name(player),
                    'STATUS': status,
                    'PLAYER_ID': player_id

                })

            return player_status_dicts

        player_names_from_player_game_logs_df = pd.read_sql_query("SELECT * FROM player_game_logs", conn).drop_duplicates('NAME_CLEAN')

        day = datetime.now(ZoneInfo(config.TIMEZONE)).day
        month = datetime.now(ZoneInfo(config.TIMEZONE)).month
        year = datetime.now(ZoneInfo(config.TIMEZONE)).year

        injury_df = pd.DataFrame()

        for i in range(16, 3, -1):

            date = datetime(
                year, month, day, i, 0,
            )

            try:

                injury_df = injury.get_reportdata(date, return_df=True)
                break

            except Exception:
                
                continue

        if injury_df.empty:

            print(f"Could not find injuries for {year}-{month}-{day}")
            sys.exit(1)

        curr_date_changed = datetime.strftime(curr_date, "%m/%d/%Y")
        curr_injury_df = injury_df[injury_df['Game Date'] == str(curr_date_changed)]

        player_status_dict = parse_nba_injuries(curr_injury_df, player_names_from_player_game_logs_df)

        return player_status_dict

    def find_average_minutes(player_id, curr_date, game_logs, season_start_date):

        player_game_logs_before_curr_date = game_logs[
            (game_logs['GAME_DATE'] >= season_start_date) &
            (game_logs['GAME_DATE'] < curr_date) &
            (game_logs['MIN'] > 0) &
            (game_logs['PLAYER_ID'] == player_id)
        ]

        if len(player_game_logs_before_curr_date) == 0:

            average_minutes = 0
        
        else:

            average_minutes = float(player_game_logs_before_curr_date['MIN'].sum()) / len(player_game_logs_before_curr_date)

        return average_minutes
    
    config = load_config()
    
    cursor = conn.cursor()

    player_status_dict = find_today_injuries(conn, curr_date)

    check_for_existing = pd.read_sql_query("SELECT * FROM DNPS WHERE GAME_DATE = ?", conn, params=(str(curr_date),))

    if not check_for_existing.empty:

        print(f"DNPS are already updated today ({curr_date})")
        user_input = input("Do you want to find dnps for today again? (y/n): ")

        while user_input.lower() not in {'y', 'n'}:

            user_input = input("Do you want to find dnps for today again? (y/n): ").lower()
        
        if user_input == "n":

            return

    game_logs = pd.read_sql_query("SELECT * FROM player_game_logs", conn)

    scoreboard_df = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER", conn)
    
    team_stats = pd.read_sql_query("SELECT * FROM TEAM_STATS_2025_2026", conn)

    for player in player_status_dict:

        player_name = player['PLAYER_NAME']
        player_id = player['PLAYER_ID']


        if player['STATUS'] in {"Out", "Doubtful"}:

            curr_player_scoreboard = scoreboard_df[

                (scoreboard_df['PLAYER_ID'] == player_id) &
                (scoreboard_df['date'] == str(curr_date))
            
            ]

            if curr_player_scoreboard.empty:

                print(f"Could not find a scoreboard to roster row for {player_name}. Check parsing_nba_injuries.py Line 218")
                continue

            curr_game_id = curr_player_scoreboard['GAME_ID'].iloc[0]
            team_id = curr_player_scoreboard['TeamID'].iloc[0]
            team_name = team_stats[team_stats['TEAM_ID'] == str(team_id)]['TEAM_NAME'].iloc[0]
            average_minutes = find_average_minutes(
                player_id=player_id, 
                curr_date=str(curr_date), 
                game_logs=game_logs, 
                season_start_date=season_start_date
            )
        
            stats = [
                str(curr_date),
                curr_game_id,
                int(team_id),
                team_name,
                player_id,
                player_name,
                average_minutes,
                1
            ]

            placeholders = ", ".join(['?']*len(stats))

            cursor.execute(f"INSERT OR REPLACE INTO DNPS VALUES ({placeholders})", stats)
    
    conn.commit()

    print(f"Finished updating DNPS from nbainjuries api on {curr_date}")

def update_dnps_table(conn, season_start_date):

    config = load_config()

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

    cursor.execute("DELETE FROM DNPS WHERE FROM_NBAINJURIES = ?", (1,))
    
    check_for_last_date_updated = pd.read_sql_query("SELECT * FROM DNPS WHERE FROM_NBAINJURIES != ? ORDER BY GAME_DATE DESC", conn, params=(1,))

    latest_date_str = check_for_last_date_updated['GAME_DATE'].iloc[0]
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
    today = datetime.now(ZoneInfo(config.TIMEZONE)).date()

    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ?", conn, params=(season_start_date,))

    while curr_date < today:

        print(f"Updating DNPS sql table for {curr_date}..")

        curr_date_game_ids = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(str(curr_date),)).drop_duplicates("GAME_ID")['GAME_ID'].to_list()

        for game_id in curr_date_game_ids:

            box = boxscore.BoxScore(game_id=game_id).get_dict()

            teams = {'homeTeam': box['game']['homeTeam'], 'awayTeam': box['game']['awayTeam']}

            for venue, venue_function in teams.items():

                for player in venue_function['players']:

                    if player['status'] != "ACTIVE":

                        team_id = venue_function['teamId']
                        team_name = f"{venue_function['teamCity']} {venue_function['teamName']}"
                        player_name = player['name']
                        player_id = player['personId']
                        curr_avg_min = find_average_minutes(player_id, str(curr_date), season_game_logs)

                        stats = [str(curr_date), game_id, team_id, team_name, player_id, player_name, curr_avg_min, 0]

                        placeholders = ", ".join(['?']*len(stats))

                        cursor.execute(f"INSERT OR REPLACE INTO DNPS VALUES ({placeholders})", stats)

        curr_date += timedelta(days=1)

    conn.commit()
    print(f"Finished updating the DNPS sql table from {latest_date_str} - {curr_date}")

def update_db_gamelogs(conn):

    config = load_config()

    def clean_name(text):

        removed_accents_text =  "".join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != "Mn"
        )

        clean = removed_accents_text.replace(".", "")

        return clean

    def iso_to_number(iso):

        duration = isodate.parse_duration(iso)
        minutes = duration.total_seconds() / 60

        return int(minutes)

    def find_game_ids_by_date(date):

        gamefinder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=date,
            date_to_nullable=date
        )

        games = gamefinder.get_data_frames()[0]

        if games.empty:

            return [], 0
        
        current_games = list(games['GAME_ID'].drop_duplicates())
        current_season_id = games["SEASON_ID"].iloc[0]

        return current_games, current_season_id

    def find_reg_data(gameId, current_season_id):

        reg_boxscore = boxscore.BoxScore(game_id=gameId).get_dict()

        current_game_stats = reg_boxscore['game']

        if current_game_stats['gameStatus'] != 3:

            return -1

        home_team = current_game_stats['homeTeam']
        away_team = current_game_stats['awayTeam']

        if home_team['statistics']['points'] < away_team['statistics']['points']:

            home_team_outcome = 'L'
            away_team_outcome = 'W'

        else:

            home_team_outcome = 'W'
            away_team_outcome = 'L'
        
        reg_boxscore_stats = {}

        for team in ['homeTeam', 'awayTeam']:

            if team == 'homeTeam':

                opp_id = away_team['teamId']
                outcome = home_team_outcome
                curr_team_tricode = home_team['teamTricode']
                curr_opp_tricode = away_team['teamTricode']
                matchup = f"{curr_team_tricode} vs {curr_opp_tricode}"
            
            else:

                opp_id = home_team['teamId']
                outcome = away_team_outcome
                curr_team_tricode = away_team['teamTricode']
                curr_opp_tricode = home_team['teamTricode']
                matchup = f"{curr_team_tricode} @ {curr_opp_tricode}"

            team_stats = current_game_stats[team]

            for player in team_stats['players']:

                curr_reg_box = [

                    current_season_id,
                    team_stats['teamId'],
                    team_stats['teamTricode'],
                    " ".join([team_stats['teamCity'], team_stats['teamName']]),
                    current_game_stats['gameId'],
                    current_game_stats['gameEt'][:10],
                    matchup,
                    outcome,
                    iso_to_number(player['statistics']['minutesCalculated']),
                    player['statistics']['points'],
                    player['statistics']['fieldGoalsMade'],
                    player['statistics']['fieldGoalsAttempted'],
                    player['statistics']['fieldGoalsPercentage'],
                    player['statistics']['threePointersMade'],
                    player['statistics']['threePointersAttempted'],
                    player['statistics']['threePointersPercentage'],
                    player['statistics']['freeThrowsMade'],
                    player['statistics']['freeThrowsAttempted'],
                    player['statistics']['freeThrowsPercentage'],
                    player['statistics']['reboundsOffensive'],
                    player['statistics']['reboundsDefensive'],
                    player['statistics']['reboundsTotal'],
                    player['statistics']['assists'],
                    player['statistics']['steals'],
                    player['statistics']['blocks'],
                    player['statistics']['turnovers'],
                    player['statistics']['foulsPersonal'],
                    opp_id,
                    player['personId'],
                    player['name'],
                    player['statistics']['points'] + player['statistics']['reboundsTotal'] + player['statistics']['assists'],
                    player['statistics']['points'] + player['statistics']['reboundsTotal'],
                    player['statistics']['points'] + player['statistics']['assists'],
                    player['statistics']['reboundsTotal'] + player['statistics']['assists'],
                    player['statistics']['blocks'] + player['statistics']['steals'],
                    clean_name(player['name']),
                    int(player['starter'])

                ]
            
                reg_boxscore_stats[player['personId']] = curr_reg_box

        return reg_boxscore_stats 

    def find_pct_data(game_id, curr_date, reg_boxscore):
        
        try:

            box = boxscoreusagev3.BoxScoreUsageV3(game_id=game_id).get_dict()

        except:

            print(f"Could not find boxscoreusagev3 for {game_id}. Check player.py Line 455")
            raise
        
        print(f"Updating {game_id} on {curr_date}")

        pct_player_id_dict = {}

        for team in ['homeTeam', 'awayTeam']:

            curr = box['boxScoreUsage'][team]

            for player in curr['players']:

                player_id = player['personId']
    
                params = [
                    player['statistics']['percentageFieldGoalsMade'],
                    player['statistics']['percentageFieldGoalsAttempted'],
                    player['statistics']['percentageThreePointersMade'],
                    player['statistics']['percentageThreePointersAttempted'],
                    player['statistics']['percentageFreeThrowsMade'],
                    player['statistics']['percentageFreeThrowsAttempted'],
                    player['statistics']['percentageReboundsTotal'],
                    player['statistics']['percentageAssists'],
                    player['statistics']['percentageTurnovers'],
                    player['statistics']['percentageSteals'],
                    player['statistics']['percentageBlocks'],
                    player['statistics']['percentagePoints'],
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan
                ]
            
                pct_player_id_dict[player_id] = params
        
        return pct_player_id_dict
                    
    cursor = conn.cursor()

    # DESC - descending order
    # ASC - ascending order
    # LIMIT - how many rows SQL returns
    cursor.execute("""

                SELECT * 
                FROM player_game_logs
                ORDER BY GAME_DATE DESC 
                LIMIT 1
                   
    """)

    rows = cursor.fetchall()

    # chooses from the last updated date in the sql db
    latest_date_str = rows[0][5]
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
    today = datetime.now(ZoneInfo(config.TIMEZONE)).date()

    while curr_date < today:

        print(f"Finding gamelogs for {curr_date}..")

        current_games, current_season_id = find_game_ids_by_date(curr_date)

        if not current_games:

            print(f"Couldn't find games for {curr_date}")
            curr_date += timedelta(days=1)
            continue

        for game_id in current_games:

            arr = []
            reg_boxscore_stats = find_reg_data(game_id, current_season_id)

            if reg_boxscore_stats == -1:

                print(f"Boxscores weren't recorded for {game_id}. Check update_db_gamelogs Line 546.")
                continue

            pct_boxscore_stats = find_pct_data(game_id, str(curr_date), reg_boxscore_stats)

            for key, val in pct_boxscore_stats.items():

                reg_boxscore_stats[key].extend(val)
                arr.append(reg_boxscore_stats[key])

            for stats in arr:

                placeholders = ", ".join(["?"] * len(stats))
                query = f"INSERT OR REPLACE INTO player_game_logs VALUES ({placeholders})"

                cursor.execute(query, stats)
            
            time.sleep(0.2)
        
        curr_date += timedelta(days=1)

    
    conn.commit()

    print(f"Gamelogs updated.")

def update_scoreboard_to_team_roster(conn, current_season):

    config = load_config()

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

    cursor = conn.cursor()

    cursor.execute("""

                SELECT date
                FROM SCOREBOARD_TO_ROSTER
                ORDER BY date DESC 
                LIMIT 1
                   
    """)

    rows = cursor.fetchall()

    # chooses from the last updated date in the sql db
    latest_date_str = rows[0][0]
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
    today = datetime.now(ZoneInfo(config.TIMEZONE)).date()

    while curr_date <= today:

        check_for_existing_df = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(str(curr_date),))

        if not check_for_existing_df.empty:

            print(f"Already found a scoreboard_to_roster for {curr_date}")
            curr_date += timedelta(days=1)
            continue
        
        scoreboard_to_team_roster(current_season=current_season, curr_date=str(curr_date), conn=conn)

        curr_date += timedelta(days=1)
    
    print(f"Finished updating SCOREBOARD_TO_ROSTER SQL Table from {latest_date_str} - {today}")

def update_team_totals_per_player(conn):

    config = load_config()

    def find_team_totals_per_player_df(game_id, game_box_score):

        def find_play_by_play_stats_with_dict(play_by_play, on_court_stats_df, game_rotation):

            court_stats_df_to_dict = on_court_stats_df.to_dict(orient='records')            
            subbed_times = set()
            cache = set()

            for team in game_rotation:

                for subbed_out_time in team['IN_TIME_REAL'].to_list():

                    subbed_times.add(subbed_out_time)
        
            curr_points_total = 0

            court_stats_dict = {}

            on_court_set = set()

            for player in court_stats_df_to_dict:

                court_stats_dict[player['PLAYER_ID']] = player
            
            play_by_play = play_by_play.to_dict(orient='records')

            court_start_times = {
                                    1: 7200.0,
                                    2: 14400.0,
                                    3: 21600.0,
                                    4: 28800.0,
                                    5: 31800.0,
                                    6: 34800.0,
                                    7: 37800.0,
                                    8: 40800.0
                                }

            for play in play_by_play:

                quarter = play['period']

                curr_time = play['clock']
                minute_to_tenth_second = float(curr_time[2:4])*600
                tenth_second = float(str(curr_time[5:7]) + str(curr_time[-3:-1]))*0.1
                curr_in_time_real = court_start_times[quarter] - (minute_to_tenth_second + tenth_second)

                print(f"Finding stats for play {play['actionNumber']}...")

                if curr_in_time_real in subbed_times and curr_in_time_real not in cache:

                    for team in game_rotation:

                        team_id = float(team['TEAM_ID'].iloc[0])
                        
                        subbed_in = team[team['IN_TIME_REAL'] == curr_in_time_real]
                        player_ids = subbed_in['PERSON_ID'].to_list()

                        for player_id in player_ids:

                            print(f"{player_id} was subbed in.")
                            on_court_set.add((player_id, team_id))

                        subbed_out = team[team['OUT_TIME_REAL'] == curr_in_time_real]
                        player_ids = subbed_out['PERSON_ID'].to_list()

                        for player_id in player_ids:
                            
                            print(f"{player_id} was subbed out.")
                            on_court_set.discard((player_id, team_id))
                    
                    if len(on_court_set) > 10:

                        return pd.DataFrame()
                    
                    cache.add(curr_in_time_real)
                
                curr_team_id = play['teamId']

                if play['isFieldGoal'] == 1:

                    if play['shotValue'] == 3:

                        if play['shotResult'] == "Made":

                            if play['description'][-4:-1] == 'AST':
                                
                                for pid, curr_player_team_id in on_court_set:
                                    
                                    if curr_team_id == curr_player_team_id:

                                        court_stats_dict[pid]['AST'] += 1
                                        court_stats_dict[pid]['PTS_AST'] += 4
                                        court_stats_dict[pid]['REB_AST'] += 1
                                        court_stats_dict[pid]['FGA'] += 1
                                        court_stats_dict[pid]['FG3A'] += 1
                                        court_stats_dict[pid]['FG3M'] += 1
                                        court_stats_dict[pid]['FGM'] += 1
                                        court_stats_dict[pid]['PTS'] += 3
                                        court_stats_dict[pid]['PRA'] += 4
                                        court_stats_dict[pid]['PTS_REB'] += 3
                                        court_stats_dict[pid]['PTS_AST'] += 4
                            
                            else:

                                for pid, curr_player_team_id in on_court_set:

                                    if curr_team_id == curr_player_team_id:

                                        court_stats_dict[pid]['FGA'] += 1
                                        court_stats_dict[pid]['FG3A'] += 1
                                        court_stats_dict[pid]['FG3M'] += 1
                                        court_stats_dict[pid]['FGM'] += 1
                                        court_stats_dict[pid]['PTS'] += 3
                                        court_stats_dict[pid]['PRA'] += 3
                                        court_stats_dict[pid]['PTS_REB'] += 3
                                        court_stats_dict[pid]['PTS_AST'] += 3

                            curr_points_total += 3
                        
                        elif play['shotResult'] == 'Missed':

                            for pid, curr_player_team_id in on_court_set:
                                
                                if curr_team_id == curr_player_team_id:

                                    court_stats_dict[pid]['FGA'] += 1
                                    court_stats_dict[pid]['FG3A'] += 1
                    
                    elif play['shotValue'] == 2:

                        if play['shotResult'] == "Made":

                            if play['description'][-4:-1] == 'AST':
                                
                                for pid, curr_player_team_id in on_court_set:

                                    if curr_team_id == curr_player_team_id:

                                        court_stats_dict[pid]['AST'] += 1
                                        court_stats_dict[pid]['PTS_AST'] += 3
                                        court_stats_dict[pid]['REB_AST'] += 1
                                        court_stats_dict[pid]['FGA'] += 1
                                        court_stats_dict[pid]['FGM'] += 1
                                        court_stats_dict[pid]['PTS'] += 2
                                        court_stats_dict[pid]['PRA'] += 3
                                        court_stats_dict[pid]['PTS_REB'] += 2
                                        court_stats_dict[pid]['PTS_AST'] += 3

                            else:

                                for pid, curr_player_team_id in on_court_set:

                                    if curr_team_id == curr_player_team_id:

                                        court_stats_dict[pid]['FGA'] += 1
                                        court_stats_dict[pid]['FGM'] += 1
                                        court_stats_dict[pid]['PTS'] += 2
                                        court_stats_dict[pid]['PRA'] += 2
                                        court_stats_dict[pid]['PTS_REB'] += 2
                                        court_stats_dict[pid]['PTS_AST'] += 2

                            curr_points_total += 2
                        
                        elif play['shotResult'] == 'Missed':

                            for pid, curr_player_team_id in on_court_set:
                                
                                if curr_team_id == curr_player_team_id:
                                    
                                    court_stats_dict[pid]['FGA'] += 1

                elif play['actionType'] == 'Free Throw':

                    if play['pointsTotal'] > curr_points_total:

                        for pid, curr_player_team_id in on_court_set:
                            
                            if curr_team_id == curr_player_team_id:

                                court_stats_dict[pid]['FTA'] += 1                    
                                court_stats_dict[pid]['FTM'] += 1 
                                court_stats_dict[pid]['PTS'] += 1                   
                                court_stats_dict[pid]['PRA'] += 1                    
                                court_stats_dict[pid]['PTS_REB'] += 1                    
                                court_stats_dict[pid]['PTS_AST'] += 1

                        curr_points_total += 1
        
                    else:

                        for pid, curr_player_team_id in on_court_set:

                            if curr_team_id == curr_player_team_id:

                                court_stats_dict[pid]['FTA'] += 1

                elif play['actionType'] == 'Rebound':

                    for pid, curr_player_team_id in on_court_set:

                        if curr_team_id == curr_player_team_id:

                            court_stats_dict[pid]['REB'] += 1
                            court_stats_dict[pid]['PRA'] += 1
                            court_stats_dict[pid]['PTS_REB'] += 1
                            court_stats_dict[pid]['REB_AST'] += 1

                elif 'STEAL' in play['description']:
                    
                    for pid, curr_player_team_id in on_court_set:

                        if curr_team_id == curr_player_team_id:

                            court_stats_dict[pid]['STL'] += 1

                elif 'BLOCK' in play['description']:

                    for pid, curr_player_team_id in on_court_set:

                        if curr_team_id == curr_player_team_id:

                            court_stats_dict[pid]['BLK'] += 1   

            dfs = []

            for pid, player_dict in court_stats_dict.items():

                dfs.append(pd.DataFrame([player_dict]))
            
            return pd.concat(dfs, ignore_index=True)
        
        play_by_play = playbyplayv3.PlayByPlayV3(game_id=game_id).get_data_frames()[0]
        time.sleep(1)
        game_rotation = GameRotation(game_id=game_id).get_data_frames()

        player_ids = game_box_score['PLAYER_ID'].to_list()

        curr_date = game_box_score['GAME_DATE'].iloc[0]

        dfs = []
        for pid in player_ids:

            curr_team_id = game_box_score[game_box_score['PLAYER_ID'] == pid]['TEAM_ID'].iloc[0]
            player_name = game_box_score[game_box_score['PLAYER_ID'] == pid]['PLAYER_NAME'].iloc[0]

            dfs.append(pd.DataFrame([{
                'GAME_DATE': curr_date,
                'GAME_ID': game_id,
                'TEAM_ID': int(curr_team_id),
                'PLAYER_ID': pid,
                'PLAYER_NAME': player_name
            }]))

        rosters = pd.concat(dfs, ignore_index=True)

        col_names = ['GAME_DATE', 'GAME_ID', 'PLAYER_NAME', 'PLAYER_ID', 'PTS', 'REB', 'AST', 'STL', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'BLK', 'PRA', 'PTS_REB', 'PTS_AST', 'REB_AST']

        on_court_stats_df = pd.DataFrame(columns=col_names)

        on_court_stats_df = pd.concat([on_court_stats_df, rosters], ignore_index=True)

        # finds all cells where it doesn't equal NaN, and replaces 0 with all the other cells
        on_court_stats_df = on_court_stats_df.where(on_court_stats_df.notna(), 0)
        on_court_stats_df = find_play_by_play_stats_with_dict(play_by_play, on_court_stats_df, game_rotation)

        if on_court_stats_df.empty:

            return pd.DataFrame()

        on_court_stats_df = on_court_stats_df.sort_values("PLAYER_NAME").reset_index(drop=True)

        return on_court_stats_df

    latest_date_str = pd.read_sql_query("SELECT * FROM TEAM_TOTALS_PER_PLAYER ORDER BY GAME_DATE DESC", conn)['GAME_DATE'].iloc[0]
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date() + timedelta(days=1)
    today = datetime.now(ZoneInfo(config.TIMEZONE)).date()

    cursor = conn.cursor()

    corrupted = []

    while curr_date < today:

        curr_day_boxscores = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE = ?", conn, params=(str(curr_date),))

        if curr_day_boxscores.empty:

            print(f"Could not find game logs for {curr_date} from player_game_logs table.")
            curr_date += timedelta(days=1)
            continue
            
        game_ids = curr_day_boxscores.drop_duplicates('GAME_ID')['GAME_ID'].to_list()

        for i in range(len(game_ids)):
            
            corrupted_game_id = False

            while True:

                try:

                    print(f"Finding team totals per player for {game_ids[i]} on {curr_date} ({i+1} / {len(game_ids)})")

                    curr_box_score = curr_day_boxscores[curr_day_boxscores['GAME_ID'] == game_ids[i]]
                    curr_df = find_team_totals_per_player_df(game_id=game_ids[i], game_box_score=curr_box_score)

                    if curr_df.empty:

                        print(f"Something is wrong with the game rotations for {game_ids[i]}. Check players.py Line 1105")
                        corrupted.append(game_ids[i])
                        corrupted_game_id = True
                    
                    break
                
                except Timeout as t:

                    print(f"Failed on {game_ids[i]} due to {t}")
                    continue

                except Exception as e:

                    print(f"Failed on {game_ids[i]} due to {e}")
                    raise
            
            if corrupted_game_id:

                continue

            curr_df = curr_df.drop(columns=['TEAM_ID'])
            curr_df = curr_df.to_dict(orient='records')

            for hashmap in curr_df:

                stats = list(hashmap.values())
                col_names = list(hashmap.keys())

                stats_col_names = list(zip(stats, col_names))

                for stat, col_name in stats_col_names:

                    if col_name == 'PLAYER_ID':

                        curr_pid = stat
                    
                    if col_name == 'GAME_ID':

                        curr_gid = stat

                player_box_score = curr_day_boxscores[curr_day_boxscores['PLAYER_ID'] == curr_pid]

                for curr_team_total_per_player_stat, stat_name in stats_col_names:

                    if curr_team_total_per_player_stat == 0:

                        continue

                    if stat_name in {'PRA', 'PTS_REB', 'PTS_AST', 'REB_AST'}:

                        stat_recorded = player_box_score[stat_name].iloc[0]

                        curr_col_name = f"PCT_{stat_name}_USAGE"

                        cursor.execute(f"""

                            UPDATE player_game_logs
                            SET 
                                {curr_col_name} = ?
                            WHERE PLAYER_ID = ?
                            AND GAME_ID = ?

                        """, (float(int(stat_recorded) / curr_team_total_per_player_stat), curr_pid, curr_gid))

                player_id = hashmap['PLAYER_ID']
                minutes = curr_box_score[curr_box_score['PLAYER_ID'] == player_id]['MIN'].iloc[0]
                stats.append(int(minutes))
                col_names.append('MIN')

                placeholders = ", ".join(['?']*len(stats))
                col_names = ", ".join(col_names)

                cursor.execute(f"""

                    INSERT OR REPLACE INTO TEAM_TOTALS_PER_PLAYER ({col_names})
                    VALUES ({placeholders})

                """, stats)
            
            time.sleep(1)
            conn.commit()
        
        curr_date += timedelta(days=1)
    
    with open(f"corrupted_GameRotation.json", "r") as f:

        all_corrupted = json.load(f)
    
    for game_id in corrupted:

        if game_id not in all_corrupted:

            all_corrupted.append(game_id)
    
    with open(f"corrupted_GameRotation.json", "w") as f:

        json.dump(all_corrupted, f, indent=4)

    print(f"Finished updating team totals per player sql table.")

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_PATH)
    season_start_date = '2025-10-21'

    update_team_totals_per_player(conn=conn)