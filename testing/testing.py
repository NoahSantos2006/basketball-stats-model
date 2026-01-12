import sqlite3
import pandas as pd

from nba_api.live.nba.endpoints import boxscore

from basketball_stats_bot.config import load_config

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()

    game_id = "0022500554"
    box = boxscore.BoxScore(game_id=game_id)

    print(box)
    