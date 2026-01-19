import pandas as pd
import sys
import json
import time
import os
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
import numpy as np
from io import StringIO

from basketball_stats_bot.config import load_config
from basketball_stats_bot.programs.updating_functions.players import update_dnps_from_nbainjuries
from basketball_stats_bot.programs.main_functions.props import props_parser, player_vs_prop_scores, get_prop_lines, get_today_ids
from basketball_stats_bot.programs.main_functions.nba_api_database import scoreboard_to_team_roster, player_vs_team_or_last_20
from basketball_stats_bot.programs.main_functions.updatingDB import updateDB
from basketball_stats_bot.programs.main_functions.result import result

## main stuff
if __name__ == "__main__":

    config = load_config()
    current_season = "2025-26"
    season = "2025_2026"

    start = time.time()

    date = datetime.now(ZoneInfo(config.TIMEZONE)).date()

    print(f"Date: {date}")

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    # updates sqlite db
    user_input = input("Do you want to update the sql database? (y/n): ")

    while user_input.lower() not in {'y', 'n'}:

        user_input = input("Do you want to update the sql database? (y/n): ")

    if user_input == "y":
        
        updateDB(config.API_KEY, date, config.SEASON_START_DATE, conn, current_season)

    get_lines = input("Do you want to retrieve player prop lines? y/n: ").lower()

    while get_lines not in {"y", "n"}:

        get_lines = input("Do you want to retrieve player prop lines? y/n: ").lower()
    
    only_want_game_logs = 'n'
    
    # the user doesn't want prop lines
    if get_lines == "n":

        only_want_game_logs = input("Do you just want to get player game logs? y/n: ").lower()

        while only_want_game_logs not in {"y", "n"}:

            only_want_game_logs = input("Do you just want get player game logs? y/n: ").lower()
        
    scoreboard_to_team_roster_df = scoreboard_to_team_roster(current_season, date, conn)

    player_vs_team_or_last_20_df = player_vs_team_or_last_20(scoreboard_to_team_roster_df, date, config.SEASON_START_DATE, conn)
    
    if only_want_game_logs == "y":

        end = time.time()
        elapsed = end - start

        print(f"Only found player game logs. Elapsed: {elapsed:.2f}")
        exit()

    if get_lines == "n":

        user_input = input("\nDo you already have the prop lines and want to parse them for the system? y/n: ")

        while user_input not in {"y", "n"}:

            user_input = input("\nDo you already have the prop lines and want to parse them for the system? y/n: ")

        if user_input == "y":

            update_dnps_from_nbainjuries(conn, config.SEASON_START_DATE, date)

            all_game_event_odds_row = pd.read_sql_query("SELECT * FROM ODDS_API WHERE DATE = ?", conn, params=(str(date),))

            if all_game_event_odds_row['GAME_PROPS'].iloc[0] not in {np.nan, None}:

                all_game_event_odds = pd.read_json(StringIO(all_game_event_odds_row['GAME_PROPS'].iloc[0]))
            
            else:

                print(f"Could not find game props for {date}. Check ODDS_API sql database.")
                sys.exit(1)

            player_vs_team_or_last_20_row = pd.read_sql_query("SELECT * FROM PLAYER_VS_TEAM_OR_LAST_20_JSONS WHERE DATE = ?", conn, params=(str(date),))

            if not player_vs_team_or_last_20_row.empty:

                player_vs_team_or_last_20_df = pd.read_json(StringIO(player_vs_team_or_last_20_row['JSON_FILE'].iloc[0]))
            
            else:

                print(f"Could not find a player vs team or last 20 json file for {date}. Check PLAYER_VS_TEAM_OR_LAST_20_JSONS SQL database.")


            draftkings_sportsbook = props_parser(all_game_event_odds, conn)

            scores = player_vs_prop_scores(player_vs_team_or_last_20_df, draftkings_sportsbook, date, conn, config.SEASON_START_DATE, season)

            system = result(scores, date, conn)

            end = time.time()
            elapsed = end - start

            print(f"All data retrieved. Elapsed Time: {elapsed:.2f}")
            exit()

        end = time.time()
        elapsed = end - start

        exit()

    today_ids = get_today_ids(config.API_KEY, conn)

    all_game_event_odds = get_prop_lines(date, today_ids, config.API_KEY, conn)

    draftkings_sportsbook = props_parser(all_game_event_odds, conn)

    scores = player_vs_prop_scores(player_vs_team_or_last_20_df, draftkings_sportsbook, date, conn, config.SEASON_START_DATE, season)

    system = result(scores, date, conn)



    end = time.time()
    elapsed = end - start

    print(f"All data was found. Elapsed Time: {elapsed:.2f}")
