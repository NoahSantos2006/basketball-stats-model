import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV, TimeSeriesSplit
from sklearn.metrics import ConfusionMatrixDisplay
from xgboost import XGBClassifier, XGBRegressor
import xgboost as xgb
import matplotlib.pyplot as plt
import joblib
import numpy as np

def train_minutes_projection_model(conn):

    print(f"Training Minutes Projection Model...")

    df = pd.read_sql_query("SELECT * FROM MINUTES_PROJECTION_TRAINING", conn)

    # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
    df = df.sort_values("GAME_DATE").reset_index(drop=True)

    feature_cols = [
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

    df = df.dropna(subset=["MINUTES"])
    
    X = df[feature_cols]
    y = df["MINUTES"]

    reg_xgb = xgb.XGBRegressor(
                                    objective="reg:squarederror",
                                    random_state=42, # sets random seed for row subsampling, colum subsampling and tree construction randomness
                                    eval_metric='mae',
                                    tree_method='hist', # histogram-based tree construction
                            )
    
    param_dist = {
        "max_depth": [3, 4, 5], # model complexity
        "learning_rate": [0.03, 0.05, 0.1], # shrinks each tree's contribution, lower = more stable, needs more trees
        "min_child_weight": [1, 5, 10, 20], # controls how much data a tree split must have before XGboost is allowed to create it
        "n_estimators": [300, 500, 800, 1200], # of trees
        "gamma": [0, 0.1, 0.25, 0.5, 1.0], # minimum loss reduction to split
        "subsample": [0.7, 0.9], # % of rows per tree
        "colsample_bytree": [0.5, 0.7], # % of features per tree
        "reg_lambda": [0.5, 1.0, 2.0, 5.0],
        "reg_alpha": [0, 0.1, 0.5, 1.0],
    }

    tscv = TimeSeriesSplit(n_splits=5)

    search = RandomizedSearchCV(
        estimator=reg_xgb, # base model the RSC will tune
        param_distributions=param_dist, 
        n_iter=50, # how many differeny hyperparameter combinations to try
        scoring="neg_mean_absolute_error", # scoring metric used to decide which model is "best"
        cv=tscv, # always trains on past and tests on future
        n_jobs=10, # how many CPU cores to use in parallel
        random_state=42, # seed for randomness
        verbose=1 # controls how much output you see
    )

    search.fit(X,y)

    best_params = search.best_params_

    final_model = XGBRegressor(
        **best_params,
        objective="reg:squarederror",
        random_state=42,
        eval_metric='mae',
        tree_method='hist',
    )

    final_model.fit(X,y)

    joblib.dump(final_model, fr"C:\Users\noahs\.vscode\basketball stats bot\main\training\models\XGBoost\minutes_projection_model.pkl")

if __name__ == "__main__":

    con = sqlite3.connect(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")

    train_minutes_projection_model(con)