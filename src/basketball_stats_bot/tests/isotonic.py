import sqlite3
import pandas as pd
from sklearn.isotonic import IsotonicRegression
import numpy as np
import matplotlib.pyplot as plt
from basketball_stats_bot.config import load_config

config = load_config()

con = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

system = pd.read_sql_query("SELECT * FROM SYSTEM WHERE DATE >= ? AND DATE <= ?", con, params=("2026-01-10", "2026-01-16"))
y_prob_val = system[system['SCORE'] > 60]['SCORE']
y_val = system[system['SCORE'] > 60]['RESULT']

iso = IsotonicRegression(out_of_bounds="clip")
iso.fit(y_prob_val, y_val)

p = np.linspace(0, 1, 100)

p_calibrated = iso.transform(p)

plt.figure(figsize=(6, 6))

# Perfect calibration
plt.plot([0, 1], [0, 1], linestyle="--", label="Perfect calibration")

# Isotonic regression curve
plt.plot(p, p_calibrated, label="Isotonic regression")

plt.xlabel("Predicted probability (XGBoost)")
plt.ylabel("Calibrated probability")
plt.title("Isotonic Calibration Curve")
plt.legend()
plt.grid(True)

plt.show()
