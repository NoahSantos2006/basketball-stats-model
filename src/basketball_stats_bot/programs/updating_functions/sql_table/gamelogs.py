import sqlite3
from nba_api.stats.endpoints import leaguegamefinder, boxscoreusagev3
from nba_api.live.nba.endpoints import boxscore
from datetime import date, timedelta, datetime
import isodate
import unicodedata
import pandas as pd
from zoneinfo import ZoneInfo
import numpy as np
import json
import sys
import time
from requests import Timeout

from basketball_stats_bot.config import load_config


def update_db_gamelogs(conn):

    config = load_config()
    corrupted_boxscore  = []
    corrupted_boxscoreusage  = []

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

        try: 
            gamefinder = leaguegamefinder.LeagueGameFinder(
                date_from_nullable=date,
                date_to_nullable=date
            )
        except json.decoder.JSONDecodeError as j:

            print(j)
            return [], 0

        games = gamefinder.get_data_frames()[0]

        if games.empty:

            return [], 0
        
        current_games = list(games['GAME_ID'].drop_duplicates())
        current_season_id = games["SEASON_ID"].iloc[0]

        return current_games, current_season_id

    def find_reg_data(gameId, current_season_id):

        while True:

            try:

                reg_boxscore = boxscore.BoxScore(game_id=gameId, timeout=10).get_dict()
                break
            
            except json.decoder.JSONDecodeError as j:

                corrupted_boxscore.append(gameId)
                return -1
            
            except Timeout as t:
                
                print(t)
                time.sleep(5)
                continue
            
            except:
                
                with open(f"corrupted_BoxScore_{start_date_str}-{curr_date}.json", "w") as f:

                    json.dump(corrupted_boxscore, f, indent=4)

                with open(f"corrupted_BoxScoreUsage_{start_date_str}-{curr_date}.json", "w") as f:

                    json.dump(corrupted_boxscore, f, indent=4)
                
                print(f"Failed on {gameId}")

                conn.close()
                raise


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
        
        while True:

            try:

                box = boxscoreusagev3.BoxScoreUsageV3(game_id=game_id, timeout=10).get_dict()
                break

            except Timeout as t:

                print(t)
                time.sleep(5)
                continue

            except json.decoder.JSONDecodeError as j:

                print(f"Could not find boxscoreusagev3 for {game_id}. Check player.py Line 455")
                corrupted_boxscoreusage.append(game_id)
                return -1

            except:

                with open(f"corrupted_BoxScore_{start_date_str}-{curr_date}.json", "w") as f:

                    json.dump(corrupted_boxscore, f, indent=4)

                with open(f"corrupted_BoxScoreUsage_{start_date_str}-{curr_date}.json", "w") as f:

                    json.dump(corrupted_boxscore, f, indent=4)
                conn.close()
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
    start_date_str = "2023-03-16"
    curr_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime("2023-06-12", "%Y-%m-%d").date()

    while curr_date <= end_date:

        print(f"Finding gamelogs for {curr_date}..")

        current_games, current_season_id = find_game_ids_by_date(curr_date)

        if not current_games:

            print(f"Couldn't find games for {curr_date}")
            curr_date += timedelta(days=1)
            continue

        for game_id in current_games:

            if game_id[:2] != "00":

                game_id = "00" + game_id

            arr = []
            reg_boxscore_stats = find_reg_data(game_id, current_season_id)

            if reg_boxscore_stats == -1:

                print(f"Boxscores weren't recorded for {game_id}. Check update_db_gamelogs Line 546.")
                continue
            
            time.sleep(0.4)
            pct_boxscore_stats = find_pct_data(game_id, str(curr_date), reg_boxscore_stats)

            if pct_boxscore_stats == -1:

                print(f"Boxscores usages weren't recorded for {game_id}. Check update_db_gamelogs Line 546.")
                continue

            for key, val in pct_boxscore_stats.items():

                if key not in reg_boxscore_stats:

                    print(f"Could not find {key} in reg box score stats")
                    continue

                reg_boxscore_stats[key].extend(val)
                arr.append(reg_boxscore_stats[key])

            for stats in arr:

                placeholders = ", ".join(["?"] * len(stats))
                query = f"INSERT OR REPLACE INTO player_game_logs VALUES ({placeholders})"

                cursor.execute(query, stats)
            
            
            time.sleep(0.2)
        
        conn.commit()
        curr_date += timedelta(days=1)


    with open(f"corrupted_BoxScore_{start_date_str}-{end_date}.json", "w") as f:

        json.dump(corrupted_boxscore, f, indent=4)

    with open(f"corrupted_BoxScoreUsage_{start_date_str}-{end_date}.json", "w") as f:

        json.dump(corrupted_boxscore, f, indent=4)

    print(f"Gamelogs updated.")

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    update_db_gamelogs(conn)