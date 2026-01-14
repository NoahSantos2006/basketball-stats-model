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

    model = joblib.load(r"C:\Users\noahs\.vscode\basketball_stats_model\basketball-stats-model\src\basketball_stats_bot\data\XGBoost\minutes_projection_model.pkl")

    xgb.plot_importance(model)
    plt.show()
    
    






    