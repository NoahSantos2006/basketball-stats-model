import sqlite3
import pandas as pd
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from xgboost import XGBClassifier
import xgboost as xgb
import matplotlib.pyplot as plt
import joblib

from basketball_stats_bot.config import load_config

def train_xgboost_without_nan_model(conn):

    def train_model(conn, prop):

        df = pd.read_sql_query("SELECT * FROM TRAINING_TABLE WHERE PROP = ?", conn, params=(prop,))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

        feature_cols = [
            "PROP_LINE",
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
            "OPP_GAME_COUNT"
        ]

        X = df[feature_cols]
        y = df["TARGET"]

        clf_xgb = xgb.XGBClassifier(
                                        objective='binary:logistic', # binary classification problem (0s and 1s) uses logistic loss
                                        random_state=42, # sets random seed for row subsampling, colum subsampling and tree construction randomness
                                        eval_metric='auc', # Area Under the Precision-Recall Curve (focuses on positive-class performance)
                                        tree_method='hist', # histogram-based tree construction
                                )
        
        param_dist = {
            "max_depth": [3, 4, 5], # model complexity
            "learning_rate": [0.03, 0.05, 0.1], # shrinks each tree's contribution, lower = more stable, needs more trees
            "min_child_weight": [10, 20, 40], # controls how much data a tree split must have before XGboost is allowed to create it
            "n_estimators": [150, 250, 350, 600, 1000], # of trees
            "gamma": [0, 0.25, 1.0], # minimum loss reduction to split
            "subsample": [0.7, 0.9], # % of rows per tree
            "colsample_bytree": [0.5, 0.7] # % of features per tree
        }

        tscv = TimeSeriesSplit(n_splits=5)

        search = RandomizedSearchCV(
            estimator=clf_xgb, # base model the RSC will tune
            param_distributions=param_dist, 
            n_iter=50, # how many differeny hyperparameter combinations to try
            scoring="roc_auc", # scoring metric used to decide which model is "best"
            cv=tscv, # always trains on past and tests on future
            # cv=5, # training data is divided into 5 equal parts and for each hyperparameter combination, the model is trained 5 times (4 for training 1 for validation)
            n_jobs=10, # how many CPU cores to use in parallel
            random_state=42, # seed for randomness
            verbose=1 # controls how much output you see
        )

        search.fit(X,y)

        best_params = search.best_params_

        final_model = XGBClassifier(
            **best_params,
            objective="binary:logistic",
            random_state=42,
            eval_metric='aucpr',
            tree_method='hist',
            scale_pos_weight= (len(y) - y.sum()) / y.sum()
        )

        final_model.fit(X,y)

        joblib.dump(final_model, fr"C:\Users\noahs\.vscode\basketball stats bot\main\training\models\XGBoost\{prop}_xgboost_model.pkl")
    
    props = [

        'PTS',
        'REB',
        'AST',
        'STL',
        'BLK',
        'FG3M',
        'PRA',
        'PTS_REB',
        'PTS_AST',
        'REB_AST'

    ]
        
    for prop in props:
        
        print(f"Training XGBoost model without NAN for {prop}..")
        train_model(conn, prop)

if __name__ == "__main__":

    config = load_config()
    con = sqlite3.connect(config.DB_PATH)

    train_xgboost_without_nan_model(con)