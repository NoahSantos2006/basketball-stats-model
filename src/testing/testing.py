import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import joblib
import sys
import numpy as np


from xgboost import plot_importance
import xgboost as xgb

from nba_api.live.nba.endpoints import boxscore

from basketball_stats_bot.config import load_config

if __name__ == "__main__":

    config = load_config()

    con = sqlite3.connect(config.DB_ONE_DRIVE_PATH)
    cur = con.cursor()

    cur.execute("DELETE FROM PLAYER_VS_TEAM_OR_LAST_20_JSONS WHERE DATE IS NULL")
    con.commit()
    
    
    






    