import pandas as pd
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
import joblib
import xgboost as xgb
from xgboost import XGBClassifier, XGBRegressor
import sqlite3
import os
import sys
from datetime import datetime, timedelta


from basketball_stats_bot.config import load_config

def find_best_params_for_v7(conn, curr_date):

    config = load_config()

    def find_best_params(conn, prop, date):

        df = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE PROP = ? AND GAME_DATE < ?", conn, params=(prop, str(date)))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

        df = df[df['GAME_DATE'] < "2025-12-10"]


        feature_cols = [
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
            n_jobs=10, # how many CPU cores to use in parallel
            random_state=42, # seed for randomness
            verbose=1 # controls how much output you see
        )

        search.fit(X,y)

        best_params = search.best_params_

        testing_best_params_path = os.path.join(config.XGBOOST_PATH, 'testing_best_params', 'v7')

        if not os.path.isdir(testing_best_params_path):

            os.mkdir(testing_best_params_path)

        curr_path = os.path.join(testing_best_params_path, f"{curr_date}_{prop}_v7_best_params.pkl")

        joblib.dump(best_params, curr_path)
    
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
        
        print(f"Finding best params for XGBoost Model with usage values for {prop}.. ({curr_date})")
        find_best_params(conn, prop, curr_date)

def find_best_params_for_v8(conn, curr_date):

    config = load_config()

    def find_best_params(conn, prop, date):

        df = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE PROP = ? AND GAME_DATE < ?", conn, params=(prop, str(date)))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

        df = df[df['GAME_DATE'] < "2025-12-05"]


        feature_cols = [
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
            n_jobs=10, # how many CPU cores to use in parallel
            random_state=42, # seed for randomness
            verbose=1 # controls how much output you see
        )

        search.fit(X,y)

        best_params = search.best_params_

        testing_best_params_path = os.path.join(config.XGBOOST_PATH, 'testing_best_params', 'v8')

        if not os.path.isdir(testing_best_params_path):

            os.mkdir(testing_best_params_path)

        curr_path = os.path.join(testing_best_params_path, f"{curr_date}_{prop}_v8_best_params.pkl")

        joblib.dump(best_params, curr_path)
    
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
        
        print(f"Finding best params for XGBoost Model with usage values for {prop}.. ({curr_date})")
        find_best_params(conn, prop, curr_date)

def find_best_params_for_v9(conn, curr_date):

    config = load_config()

    def find_best_params(conn, prop, date):

        df = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE PROP = ? AND GAME_DATE < ?", conn, params=(prop, str(date)))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

        df = df[df['GAME_DATE'] < "2025-12-05"]

        feature_cols = [
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
            n_jobs=10, # how many CPU cores to use in parallel
            random_state=42, # seed for randomness
            verbose=1 # controls how much output you see
        )

        search.fit(X,y)

        best_params = search.best_params_

        testing_best_params_path = os.path.join(config.XGBOOST_PATH, 'testing_best_params', 'v9')

        if not os.path.isdir(testing_best_params_path):

            os.mkdir(testing_best_params_path)

        curr_path = os.path.join(testing_best_params_path, f"{curr_date}_{prop}_v9_best_params.pkl")

        joblib.dump(best_params, curr_path)
    
    props = [

        'PTS',
        'REB',
        'AST',
        'FG3M',
        'PRA',
        'PTS_REB',
        'PTS_AST',
        'REB_AST'

    ]
        
    for prop in props:
        
        print(f"Finding best params for XGBoost Model V9 on {prop}.. ({curr_date})")
        find_best_params(conn, prop, curr_date)

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    curr_date_str = "2025-12-10"
    end_date_str = "2025-12-10"
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    while curr_date <= end_date:

        # find_best_params_for_v7(conn, curr_date)
        # find_best_params_for_v8(conn, curr_date)
        find_best_params_for_v9(conn, curr_date)

        curr_date += timedelta(days=1)
