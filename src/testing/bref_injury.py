import pandas as pd
import requests
import sqlite3
import unicodedata
import sys

from basketball_stats_bot.config import load_config

def clean_name(text):

    removed_accents_text =  "".join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != "Mn"
    )

    clean = removed_accents_text.replace(".", "")

    return clean

if __name__ == "__main__":

    df = pd.read_html("https://www.basketball-reference.com/friv/injuries.fcgi")[0]

    config = load_config()

    con = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    curr_date = "2026-01-14"

    today_scoreboard = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", con, params=(curr_date,))

    current_team_ids = set(today_scoreboard.drop_duplicates("TeamID")['TeamID'].to_list())

    team_stats = pd.read_sql_query("SELECT * FROM DEFENSE_VS_POSITION_2025_2026", con).drop_duplicates("TEAM_ID")

    game_logs = pd.read_sql_query("SELECT * FROM player_game_logs", con)


    team_names = team_stats['TEAM_NAME'].to_list()
    team_ids = team_stats['TEAM_ID'].to_list()

    team_name_ids = dict(zip(team_names, team_ids))
    team_name_ids['Los Angeles Clippers'] = 1610612746


    players = df['Player'].to_list()
    teams = df['Team'].to_list()
    desc = df['Description'].to_list()

    player_team_desc = list(zip(players, teams, desc))

    out = []

    for player_name, team, desc in player_team_desc:
        
        if team_name_ids[team] in current_team_ids:

            if desc[:3] == "Out":

                out.append((player_name, team))
        
    parsed = []
    
    for player_name, team in out:

        player_name = clean_name(player_name)

        player_game_logs = game_logs[game_logs['NAME_CLEAN'] == player_name]

        if player_game_logs.empty:

            print(f"Couldn't find game logs for {player_name}")
            sys.exit(1)
        
        player_id = player_game_logs['PLAYER_ID'].iloc[0]

        parsed.append((player_name, int(player_id)))
    
    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ?", con, params=(config.SEASON_START_DATE,))
    scoreboard_df = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER", con)
    team_stats = pd.read_sql_query("SELECT * FROM TEAM_STATS_2025_2026", con)

    for player_name, player_id in parsed:

        if curr_player_scoreboard.empty:

            print(f"Could not find a scoreboard to roster row for {player_name}. Check parsing_nba_injuries.py Line 218")
            continue

        curr_game_id = curr_player_scoreboard['GAME_ID'].iloc[0]
        team_id = curr_player_scoreboard['TeamID'].iloc[0]
        team_name = team_stats[team_stats['TEAM_ID'] == str(team_id)]['TEAM_NAME'].iloc[0]
        average_minutes = find_average_minutes(
            player_id=player_id, 
            curr_date=str(curr_date), 
            game_logs=season_game_logs, 
            season_start_date=season_start_date
        )
        curr_avg_pts = find_average_points(player_id, str(curr_date), season_game_logs)
        curr_avg_reb = find_average_rebounds(player_id, str(curr_date), season_game_logs)
        curr_avg_ast = find_average_assists(player_id, str(curr_date), season_game_logs)
        curr_avg_fg3m = find_average_FG3M(player_id, str(curr_date), season_game_logs)
        curr_avg_pra = curr_avg_pts + curr_avg_reb + curr_avg_ast
        curr_avg_pts_reb = curr_avg_pts + curr_avg_reb
        curr_avg_pts_ast = curr_avg_pts + curr_avg_ast
        curr_avg_reb_ast = curr_avg_reb + curr_avg_ast

        stats = [
            str(curr_date),
            curr_game_id,
            int(team_id),
            team_name,
            player_id,
            player_name,
            average_minutes,
            1,
            curr_avg_pts,
            curr_avg_reb,
            curr_avg_ast,
            curr_avg_fg3m,
            curr_avg_pra,
            curr_avg_pts_reb,
            curr_avg_pts_ast,
            curr_avg_reb_ast
        ]

        placeholders = ", ".join(['?']*len(stats))

        cursor.execute(f"INSERT OR REPLACE INTO DNPS VALUES ({placeholders})", stats)
    
    conn.commit()
