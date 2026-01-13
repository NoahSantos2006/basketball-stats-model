import xgboost as xgb
from xgboost import XGBClassifier, XGBRegressor
from sklearn.metrics import roc_auc_score, brier_score_loss, log_loss
import joblib
import sqlite3
import os
from datetime import datetime, timedelta
import pandas as pd
import sys
import matplotlib.pyplot as plt

from basketball_stats_bot.config import load_config

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_PATH)

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

    curr_date_str = '2025-12-25'
    end_date_str = '2025-12-25'
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    training_table = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE ORDER BY GAME_DATE ASC", conn)
    training_table = training_table.sort_values("GAME_DATE").reset_index(drop=True)

    while curr_date <= end_date:

        results = []

        for prop in props:

            usage_params_path = os.path.join(config.XGBOOST_PATH, "testing_best_params", "v9", f"2025-12-05_{prop}_v9_best_params.pkl")
            usage_params = joblib.load(usage_params_path)

            models = {

                # 'xgb_v7': xgb.XGBClassifier (
                #     **usage_params,
                #     objective="binary:logistic",
                #     random_state=42,
                #     eval_metric='auc',
                #     tree_method='hist'
                # ),

                # 'xgb_v8': xgb.XGBClassifier (
                #     **usage_params,
                #     objective="binary:logistic",
                #     random_state=42,
                #     eval_metric='auc',
                #     tree_method='hist',
                # ),

                'xgb_v9': xgb.XGBClassifier (
                    **usage_params,
                    objective="binary:logistic",
                    random_state=42,
                    eval_metric='auc',
                    tree_method='hist',
                ),

                'xgb_v13': xgb.XGBClassifier (
                    **usage_params,
                    objective="binary:logistic",
                    random_state=42,
                    eval_metric='auc',
                    tree_method='hist',
                )

            }

            train_df = training_table[training_table['GAME_DATE'] <= str(curr_date)]
            test_df = training_table[training_table['GAME_DATE'] > str(curr_date)]

            for name, model in models.items():

                print(f"Collecting {name} results for {prop} on {curr_date}..")

                if name == 'xgb_v7':

                    features = [
                        "LAST_GAME",
                        "SECOND_LAST_GAME",
                        "THIRD_LAST_GAME",
                        "FOURTH_LAST_GAME",
                        "FIFTH_LAST_GAME",
                        "AVERAGE_LAST_20",
                        "LAST_GAME_VS_OPP",
                        "SECOND_LAST_GAME_VS_OPP",
                        "THIRD_LAST_GAME_VS_OPP",
                        "FOURTH_LAST_GAME_VS_OPP",
                        "FIFTH_LAST_GAME_VS_OPP",
                        "AVERAGE_LAST_10_VS_OPP",
                        "DEF_RANK",
                        "OPP_GAME_COUNT",
                        f"AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE",
                        f"AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE"
                    ]

                elif name == 'xgb_v8':

                    features = [
                        "LAST_GAME",
                        "SECOND_LAST_GAME",
                        "THIRD_LAST_GAME",
                        "FOURTH_LAST_GAME",
                        "FIFTH_LAST_GAME",
                        "AVERAGE_LAST_20",
                        "LAST_GAME_VS_OPP",
                        "SECOND_LAST_GAME_VS_OPP",
                        "THIRD_LAST_GAME_VS_OPP",
                        "FOURTH_LAST_GAME_VS_OPP",
                        "FIFTH_LAST_GAME_VS_OPP",
                        "AVERAGE_LAST_10_VS_OPP",
                        "DEF_RANK",
                        "OPP_GAME_COUNT",
                        f"AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE",
                        f"AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE",
                        "VENUE"
                    ]
                
                elif name == 'xgb_v9':

                    features = [
                        "LAST_GAME",
                        "AVG_LAST_3_OVERALL",
                        "AVG_LAST_5_OVERALL",
                        "AVG_LAST_7_OVERALL",
                        "AVG_LAST_10_OVERALL",
                        "AVERAGE_LAST_20",
                        "LAST_GAME_VS_OPP",
                        "AVG_LAST_3_VS_OPP",
                        "AVG_LAST_7_VS_OPP",
                        "AVERAGE_LAST_10_VS_OPP",
                        "DEF_RANK",
                        "OPP_GAME_COUNT",
                        f"AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE",
                        f"AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE",
                        "VENUE"
                    ]
                
                elif name == 'xgb_v10':

                    features = [
                        "LAST_GAME",
                        "AVG_LAST_3_OVERALL",
                        "AVG_LAST_5_OVERALL",
                        "AVG_LAST_7_OVERALL",
                        "AVG_LAST_10_OVERALL",
                        "AVERAGE_LAST_20",
                        "LAST_GAME_VS_OPP",
                        "AVG_LAST_3_VS_OPP",
                        "AVG_LAST_7_VS_OPP",
                        "AVERAGE_LAST_10_VS_OPP",
                        "DEF_RANK",
                        "OPP_GAME_COUNT",
                        "GAMES_PLAYED_THIS_SEASON",
                        f"AVG_LAST_5_{prop}_SHARE",
                        f"AVG_LAST_10_{prop}_SHARE",
                        "MINUTES_PROJECTION",
                        "VENUE"
                    ]
                
                elif name == 'xgb_v11':

                    features = [
                        "LAST_GAME",
                        "SECOND_LAST_GAME",
                        "AVG_LAST_3_OVERALL",
                        "AVG_LAST_5_OVERALL",
                        "AVG_LAST_7_OVERALL",
                        "AVG_LAST_10_OVERALL",
                        "AVERAGE_LAST_20",
                        "LAST_GAME_VS_OPP",
                        "SECOND_LAST_GAME_VS_OPP",
                        "AVG_LAST_3_VS_OPP",
                        "AVG_LAST_7_VS_OPP",
                        "AVERAGE_LAST_10_VS_OPP",
                        "DEF_RANK",
                        "OPP_GAME_COUNT",
                        "GAMES_PLAYED_THIS_SEASON",
                        f"AVG_LAST_5_{prop}_SHARE",
                        f"AVG_LAST_10_{prop}_SHARE",
                        "MINUTES_PROJECTION",
                        "VENUE"
                    ]

                elif name == 'xgb_v12':

                    features = [
                        "LAST_GAME",
                        "SECOND_LAST_GAME",
                        "THIRD_LAST_GAME",
                        "AVG_LAST_5_OVERALL",
                        "AVG_LAST_7_OVERALL",
                        "AVG_LAST_10_OVERALL",
                        "AVERAGE_LAST_20",
                        "LAST_GAME_VS_OPP",
                        "SECOND_LAST_GAME_VS_OPP",
                        "THIRD_LAST_GAME_VS_OPP",
                        "AVG_LAST_7_VS_OPP",
                        "AVERAGE_LAST_10_VS_OPP",
                        "DEF_RANK",
                        "OPP_GAME_COUNT",
                        f"AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE",
                        f"AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE",
                        "VENUE"
                    ],
                
                elif name == 'xgb_v13':

                    features = [
                        "LAST_GAME",
                        "AVG_LAST_3_OVERALL",
                        "AVG_LAST_5_OVERALL",
                        "AVG_LAST_7_OVERALL",
                        "AVG_LAST_10_OVERALL",
                        "AVERAGE_LAST_20",
                        "LAST_GAME_VS_OPP",
                        "AVG_LAST_3_VS_OPP",
                        "AVG_LAST_7_VS_OPP",
                        "AVERAGE_LAST_10_VS_OPP",
                        "DEF_RANK",
                        "OPP_GAME_COUNT",
                        f"AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE",
                        f"AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE",
                        "VENUE",
                        "MINUTES_PROJECTION",
                    ]
                
                


                X_train = train_df[features]
                y_train = train_df['TARGET']

                X_test = test_df[features]
                y_test = test_df['TARGET']

                model.fit(X_train, y_train)

                y_proba = model.predict_proba(X_test)[:, 1]

                mask_high = y_proba >= 0.6
                mask_low = y_proba <= 0.4

                mask = mask_high | mask_low

                hit_rate = ((y_proba >= 0.5) == y_test)[mask].mean()

                coverage = mask.mean()

                # makes sure there's at least 10 samples so that there's enough data for scoring
                if mask.sum() < 10:
                    continue

                y_test_masked = y_test[mask]

                # nununique returns number of distinct values, so if they're all 0s or 1s then the data is probably biased or unreliable to score
                if y_test_masked.nunique() < 2:
                    continue

                y_proba_masked = y_proba[mask]

                auc = roc_auc_score(y_test_masked, y_proba_masked)
                brier = brier_score_loss(y_test_masked, y_proba_masked)
                logloss = log_loss(y_test_masked, y_proba_masked)

                results.append({
                    'DATE': str(curr_date),
                    'MODEL': name,
                    'PROP': prop,
                    'AUC SCORE': auc,
                    'BRIER SCORE': brier,
                    'LOG LOSS': logloss,
                    'HIT RATE': hit_rate,
                    'COVERAGE': coverage
                })

        curr_date += timedelta(days=1)
            
        results_df = pd.DataFrame(results)

        results_df.to_json("results_df.json", orient='records', indent=4)

        print(results_df)

        exit(1)
