import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
import joblib
import sqlite3

def train_logreg_model(conn):

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

        weights = pd.Series(model.coef_[0], index=feature_cols)

        # print(weights.sort_values(ascending=False))

        joblib.dump(model, fr"C:\Users\noahs\.vscode\basketball stats bot\main\training\models\logistic_regression\{prop}_logreg_model.pkl")
        joblib.dump(scaler, fr"C:\Users\noahs\.vscode\basketball stats bot\main\training\models\logistic_regression\{prop}_logreg_scaler.pkl")
    
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

if __name__ == "__main__":

    conn = sqlite3(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")

    train_logreg_model(conn)