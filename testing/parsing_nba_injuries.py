import json
import pandas as pd
import sqlite3
import unicodedata
from datetime import datetime, timedelta
import sys
from zoneinfo import ZoneInfo

import nbainjuries.injury as injury_mod
from nbainjuries import injury

from basketball_stats_bot.config import load_config

def update_dnps_from_nbainjuries(conn, season_start_date, curr_date):

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

        date = str(datetime.now(ZoneInfo(config.TIMEZONE)).date()).split()[0].split("-")

        injury_df = pd.DataFrame()

        for i in range(4, 1, -1):

            date = datetime.strptime(f"{year}-{month}-{day}_{i}_00PM","%Y-%m-%d_%I_%M%p")

            try:

                injury_df = injury.get_reportdata(date, return_df=True)
                break

            except Exception as e:

                continue
        
        if injury_df.empty:

            for i in range(11, 6, -1):

                date = datetime.strptime(f"{year}-{month}-{day}_{i}_00AM","%Y-%m-%d_%I_%M%p")

                try:

                    injury_df = injury.get_reportdata(date, return_df=True)
                    break

                except Exception as e:

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

                print(f"Could not find a scoreboard to roster row for {player_name}. Check parsing_nba_injuries.py")
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

if __name__ == "__main__":

    config = load_config()
    
    conn = sqlite3.connect(config.DB_PATH)
    season_start_date = "2025-10-21"

    curr_date = datetime.now(ZoneInfo(config.TIMEZONE)).date()

    update_dnps_from_nbainjuries(conn=conn, season_start_date=season_start_date, curr_date=curr_date)