import sqlite3
from nba_api.stats.endpoints import leaguegamefinder, boxscoreusagev3
from nba_api.live.nba.endpoints import boxscore
from datetime import date, timedelta, datetime
import isodate
import unicodedata
import pandas as pd
from zoneinfo import ZoneInfo
import json
import sys


def update_db_gamelogs(conn):

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

    def find_reg_data(game_id, current_season_id):

        reg_boxscore = boxscore.BoxScore(game_id=game_id).get_dict()

        current_game_stats = reg_boxscore['game']

        if current_game_stats['gameStatus'] != 3:

            return []

        home_team = current_game_stats['homeTeam']
        away_team = current_game_stats['awayTeam']

        if home_team['statistics']['points'] < away_team['statistics']['points']:

            home_team_outcome = 'L'
            away_team_outcome = 'W'

        else:

            home_team_outcome = 'W'
            away_team_outcome = 'L'
        
        dnps = []
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

    def find_pct_data(game_id):

        box = boxscoreusagev3.BoxScoreUsageV3(game_id=game_id).get_dict()
        game_date = box['meta']['time'][:10]

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
    curr_date = datetime.strptime('2025-12-22', "%Y-%m-%d").date()
    today = datetime.now(ZoneInfo("America/New_York")).date()

    while curr_date <= today:

        print(f"Finding gamelogs for {curr_date}..")

        current_games, current_season_id = find_game_ids_by_date(curr_date)

        if not current_games:

            curr_date += timedelta(days=1)
            continue

        for game_id in current_games:

            arr = []
            reg_boxscore_stats = find_reg_data(game_id, current_season_id)
            pct_boxscore_stats = find_pct_data(game_id)

            for key, val in pct_boxscore_stats.items():

                reg_boxscore_stats[key].extend(val)
                arr.append(reg_boxscore_stats[key])

            for stats in arr:

                placeholders = ", ".join(["?"] * len(stats))
                query = f"INSERT OR REPLACE INTO player_game_logs VALUES ({placeholders})"

                cursor.execute(query, stats)
        
        curr_date += timedelta(days=1)
    
    conn.commit()

    print(f"Gamelogs updated.")

if __name__ == "__main__":

    conn = sqlite3.connect(r"basketball stats bot/main/game_data/data.db")

    update_db_gamelogs(conn)