import pandas as pd
import sqlite3
import sys
import requests
import json
import unicodedata
import numpy as np

from basketball_stats_bot.config import load_config

def update_player_positions(con, year):

    def clean_name(text):

        removed_accents_text =  "".join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != "Mn"
        )

        clean = removed_accents_text.replace(".", "")

        return clean
    
    name_edge_cases = {
        "Russell Westbrook III": "Russell Westbrook",
        "PJ Washington Jr": "PJ Washington",
        "Bobby Portis Jr": "Bobby Portis",
        "Bruce Brown Jr": "Bruce Brown",
        "KJ Martin Jr": "KJ Martin",
        "GG Jackson II": "GG Jackson",
        "Xavier Tillman Sr": "Xavier Tillman",
        "Tolu Smith III": "Tolu Smith",
        "Yongxi Cui": "Cui Yongxi",
        "Terence Davis II": "Terence Davis",
        "Boo Buie": "Boo Buie III",
        "Nikola Durisic": "Nikola Đurisic"
    }

    df = pd.read_html(f"https://www.fantasypros.com/nba/stats/overall.php?year={year}")[0]

    game_logs = pd.read_sql_query("SELECT * FROM player_game_logs", con)

    players = df['Player'].to_list()

    players_split = []

    cur = con.cursor()

    for player in players:

        player = player.split("(")

        players_split.append(player)

    
    player_positions = {}
    
    for player_name, desc in players_split:

        player_name = player_name.strip()

        desc = desc.split("-")
        positions = desc[1].strip().split(')')[0].split(",")

        if positions and player_name:

            player_positions[player_name] = positions

    
    for player_name, positions in player_positions.items():

        print(f"Updating positions for {player_name}")

        player_name = clean_name(player_name)

        if player_name in name_edge_cases:

            player_name = name_edge_cases[player_name]

        player_game_logs = game_logs[game_logs['NAME_CLEAN'] == player_name]

        if player_game_logs.empty:

            print(f"Could not find player game logs for {player_name}")
            sys.exit(1)
        
        player_id = player_game_logs['PLAYER_ID'].iloc[0]

        for position in positions:

            cur.execute("""

                INSERT OR REPLACE INTO PLAYER_POSITIONS_TEMP (PLAYER_NAME, PLAYER_ID, POSITION)
                VALUES (?, ?, ?)

            """, (player_name, int(player_id), position))
    
    con.commit()

if __name__ == "__main__":

    config = load_config()

    con = sqlite3.connect(config.DB_ONE_DRIVE_PATH)
    cur = con.cursor()

    update_player_positions(con, "2024")

        

    