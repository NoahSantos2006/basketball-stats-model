import xgboost as xgb
from xgboost import XGBClassifier, XGBRegressor, plot_importance
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import sqlite3
import os
from datetime import datetime, timedelta
import pandas as pd
import sys
import matplotlib.pyplot as plt
import numpy as np

from basketball_stats_bot.config import load_config

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    props = [
        'PTS',
        'AST',
        'REB',
        'PRA',
        'FG3M',
        'PTS_REB',
        'PTS_AST',
        'REB_AST'
    ]

    curr_date_str = '2026-01-01'
    end_date_str = '2026-01-01'
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    training_table = pd.read_sql_query("SELECT * FROM MINUTES_PROJECTION_TRAINING WHERE MINUTES > 30 ORDER BY GAME_DATE ASC", conn)
    training_table = training_table.sort_values("GAME_DATE").reset_index(drop=True)

    results = []

    while curr_date <= end_date:

        usage_params_path = os.path.join(config.XGBOOST_PATH, "testing_best_params", f"minutes_projection_best_params.pkl")
        usage_params = joblib.load(usage_params_path)

        models = {

            'minutes_projection': xgb.XGBRegressor (
                **usage_params,
                objective="reg:squarederror",
                random_state=42,
                eval_metric='mae',
                tree_method='hist'
            )

        }

        train_df = training_table[training_table['GAME_DATE'] <= str(curr_date)]
        test_df = training_table[training_table['GAME_DATE'] > str(curr_date)]

        for name, model in models.items():

            features = [
                "AVERAGE_LAST_3",
                "AVERAGE_LAST_5",
                "AVERAGE_LAST_7",
                "AVERAGE_LAST_10",
                "MINUTE_TREND",
                "POSITION_MISSING_MINUTES",
                "LAST_10_STANDARD_DEVIATION",
                "DAYS_OF_REST",
                "TOTAL_GAMES_PLAYED_THIS_SEASON",
                "IS_BACK_TO_BACK",
                "GAMES_STARTED_LAST_5",
                "GAMES_PLAYED_LAST_5",
                "GAMES_PLAYED_LAST_10",
            ]
            
            
            X_train = train_df[features]
            y_train = train_df['MINUTES']

            X_test = test_df[features]
            y_test = test_df['MINUTES']

            model.fit(X_train, y_train)
            
            y_proba_train = model.predict(X_train)
            y_proba = model.predict(X_test)

            mae_train = mean_absolute_error(y_train, y_proba_train)
            rmse_train = np.sqrt(mean_squared_error(y_train, y_proba_train))
            r2_train = r2_score(y_train, y_proba_train)

            mae = mean_absolute_error(y_test, y_proba)
            rmse = np.sqrt(mean_squared_error(y_test, y_proba))
            r2 = r2_score(y_test, y_proba)

            results.append({
                'DATE': str(curr_date),
                'MODEL': name,
                'MEAN ABSOLUTE ERROR': mae,
                'MEAN SQUARED ERROR': rmse,
                'R2': r2,
            })

        curr_date += timedelta(days=1)

    results_df = pd.DataFrame(results)

    print(results_df)


