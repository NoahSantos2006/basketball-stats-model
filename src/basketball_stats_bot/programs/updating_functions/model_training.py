import pandas as pd
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
import matplotlib.pyplot as plt
import joblib
import xgboost as xgb
from xgboost import XGBClassifier, XGBRegressor, plot_importance
import sqlite3
import os
import sys

from basketball_stats_bot.config import load_config

def train_logreg_model(conn):

    config = load_config()

    def train_model(conn, prop):

        df = pd.read_sql_query("SELECT * FROM TRAINING_TABLE WHERE PROP = ?", conn, params=(prop,))

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

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        model = LogisticRegression(max_iter=5000)
        model.fit(X_scaled, y)

        curr_model_path = os.path.join(config.LOG_REG_PATH, f"{prop}_logreg_model.pkl")
        curr_scaler_path = os.path.join(config.LOG_REG_PATH, f"{prop}_logreg_scaler.pkl")

        joblib.dump(model, curr_model_path)
        joblib.dump(scaler, curr_scaler_path)
    
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
        
        print(f"Training logistic regression model for {prop}..")
        train_model(conn, prop)

def train_minutes_projection_model(conn):

    config = load_config()

    print(f"Training minutes projection model...")
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
        "max_depth": [2, 3], # model complexity
        "learning_rate": [0.03, 0.05, 0.1], # shrinks each tree's contribution, lower = more stable, needs more trees
        "min_child_weight": [10, 20, 30], # controls how much data a tree split must have before XGboost is allowed to create it
        "n_estimators": [200, 400, 600], # of trees
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

    curr_path = os.path.join(config.XGBOOST_PATH, "minutes_projection_model.pkl")

    joblib.dump(final_model, curr_path)

def train_xgboost_with_nan_model(conn):

    config = load_config()

    def train_model(conn, prop):

        df = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE PROP = ?", conn, params=(prop,))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

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

        curr_path = os.path.join(config.XGBOOST_PATH, f"{prop}_xgboost_model_with_NANS.pkl")

        joblib.dump(final_model, curr_path)
    
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
        
        print(f"Training XGBoost Model with NAN Values for {prop}..")
        train_model(conn, prop)

def train_v7_model(conn):

    config = load_config()

    def train_model(conn, prop):

        df = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE PROP = ?", conn, params=(prop,))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

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

        final_model = XGBClassifier(
            **best_params,
            objective="binary:logistic",
            random_state=42,
            eval_metric='aucpr',
            tree_method='hist',
        )

        final_model.fit(X,y)

        scoringv7_path = os.path.join(config.XGBOOST_PATH, 'scoringv7')

        if not os.path.isdir(scoringv7_path):

            os.mkdir(scoringv7_path)

        curr_path = os.path.join(config.XGBOOST_PATH, "scoringv7", f"{prop}_xgboost_model_scoring_v7.pkl")

        joblib.dump(final_model, curr_path)
    
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
        
        print(f"Training XGBoost Model V7 for {prop}..")
        train_model(conn, prop)

def train_v8_model(conn):

    config = load_config()

    def train_model(conn, prop):

        df = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE PROP = ?", conn, params=(prop,))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

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

        final_model = XGBClassifier(
            **best_params,
            objective="binary:logistic",
            random_state=42,
            eval_metric='aucpr',
            tree_method='hist',
        )

        final_model.fit(X,y)

        scoringv8_path = os.path.join(config.XGBOOST_PATH, 'scoringv8')

        if not os.path.isdir(scoringv8_path):

            os.mkdir(scoringv8_path)

        curr_path = os.path.join(config.XGBOOST_PATH, "scoringv8", f"{prop}_xgboost_model_scoring_v8.pkl")

        joblib.dump(final_model, curr_path)
    
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
        
        print(f"Training XGBoost Model V8 for {prop}..")
        train_model(conn, prop)

# main
def train_v9_model(conn):

    config = load_config()

    def train_model(conn, prop):

        df = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE PROP = ?", conn, params=(prop,))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

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
            "max_depth": [2, 3], # model complexity
            "learning_rate": [0.03, 0.05], # shrinks each tree's contribution, lower = more stable, needs more trees
            "min_child_weight": [40, 80, 120], # controls how much data a tree split must have before XGboost is allowed to create it
            "n_estimators": [100, 200, 300], # of trees
            "gamma": [1.0, 5.0], # minimum loss reduction to split
            "subsample": [0.7, 0.9], # % of rows per tree
            "colsample_bytree": [0.5, 0.7], # % of features per tree
            "reg_alpha": [1, 5, 10],
            "reg_lambda": [5, 10, 20]
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

        best_params_path = os.path.join(config.XGBOOST_PATH, "testing_best_params", f"{prop}_best_params.pkl")
        joblib.dump(best_params, best_params_path)

        final_model = XGBClassifier(
            **best_params,
            objective="binary:logistic",
            random_state=42,
            eval_metric='auc',
            tree_method='hist',
        )

        final_model.fit(X,y)

        scoringv9_path = os.path.join(config.XGBOOST_PATH, 'scoringv9')

        if not os.path.isdir(scoringv9_path):

            os.mkdir(scoringv9_path)

        curr_path = os.path.join(config.XGBOOST_PATH, "scoringv9", f"{prop}_xgboost_model_scoring_v9.pkl")

        joblib.dump(final_model, curr_path)
    
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
        
        print(f"Training XGBoost Model V9 for {prop}..")
        train_model(conn, prop)

def train_v10_model(conn):

    config = load_config()

    def train_model(conn, prop):

        df = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE WHERE PROP = ?", conn, params=(prop,))

        # makes sure there's not data leak (xgboost model sees future games) uses TimeSeriesSplit later in code so that it looks in the past and trains on the future
        df = df.sort_values("GAME_DATE").reset_index(drop=True)

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
            "MINUTES_PROJECTION",
            "POSITION_MISSING_STAT",
            f"AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE",
            f"AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE",
            "VENUE",
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
            "max_depth": [2, 3], # model complexity
            "learning_rate": [0.03, 0.05], # shrinks each tree's contribution, lower = more stable, needs more trees
            "min_child_weight": [40, 80, 120], # controls how much data a tree split must have before XGboost is allowed to create it
            "n_estimators": [100, 200, 300], # of trees
            "gamma": [1.0, 5.0], # minimum loss reduction to split
            "subsample": [0.7, 0.9], # % of rows per tree
            "colsample_bytree": [0.5, 0.7], # % of features per tree
            "reg_alpha": [1, 5, 10],
            "reg_lambda": [5, 10, 20]
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

        testing_params_path = os.path.join(config.XGBOOST_PATH, "testing_best_params")

        if not os.path.isdir(testing_params_path):

            os.mkdir(testing_params_path)

        best_params_path = os.path.join(config.XGBOOST_PATH, "testing_best_params", f"{prop}_best_params.pkl")
        joblib.dump(best_params, best_params_path)

        final_model = XGBClassifier(
            **best_params,
            objective="binary:logistic",
            random_state=42,
            eval_metric='auc',
            tree_method='hist',
        )

        final_model.fit(X,y)

        scoringv10_path = os.path.join(config.XGBOOST_PATH, 'scoringv10')

        if not os.path.isdir(scoringv10_path):

            os.mkdir(scoringv10_path)

        curr_path = os.path.join(config.XGBOOST_PATH, "scoringv10", f"{prop}_xgboost_model_scoring_v10.pkl")

        joblib.dump(final_model, curr_path)
    
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
        
        print(f"Training XGBoost Model V10 for {prop}..")
        train_model(conn, prop)

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    train_v10_model(conn=conn)
