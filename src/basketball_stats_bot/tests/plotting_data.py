import matplotlib.pyplot as plt
import sqlite3
import pandas as pd

from basketball_stats_bot.config import load_config

if __name__ == "__main__":

    config = load_config()

    con = sqlite3.connect(config.DB_ONE_DRIVE_PATH)
    cur = con.cursor()

    system = pd.read_sql_query("SELECT * FROM SYSTEM WHERE DATE >= ? and date <= ?", con, params=("2026-01-10", "2026-01-17"))

    X = system['SCORE']
    Y = system['RESULT']

    plt.scatter(X, Y)

    plt.show()