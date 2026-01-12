import sqlite3
import pandas as pd
from datetime import timedelta, datetime, date
import time
import os
import json

def find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, system):

    def find_defensive_rank(conn, positions, team_id, prop):

        rank = 0

        for position in positions:

            df = pd.read_sql_query(f"SELECT * FROM DEFENSE_VS_POSITION_2025_2026 WHERE POSITION = ? ORDER BY {prop} DESC", conn, params=(position,))

            rank += df.index[df['TEAM_ID'] == team_id][0]
        
        return rank / len(positions)

    translation = {

        "PTS": "player_points",
        "REB": "player_rebounds",
        "AST": "player_assists",
        "FG3M": "player_threes",
        "BLK": "player_blocks",
        "STL": "player_steals",
        "BLK_STL": "player_blocks_steals",
        "TOV": "player_turnovers",
        "PRA": "player_points_rebounds_assists",
        "PTS_REB": "player_points_rebounds",
        "PTS_AST": "player_points_assists",
        "REB_AST": "player_rebounds_assists",
        "FGM": "player_field_goals",
        "FTM": "player_frees_made",
        "FGA": "player_frees_attempt"

    }
    
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
    
    today = datetime.strptime(curr_date, "%Y-%m-%d").date()
    two_years_from_curr_date = str(today - timedelta(days=730))

    curr_player_game_logs = game_logs[game_logs['PLAYER_ID'] == player_id]

    player_positions_df = pd.read_sql_query("SELECT * FROM PLAYER_POSITIONS", conn)

    # finds player positions
    player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

    # finds the game_id for the player's game today
    game_id = scoreboard[
        (scoreboard['date'] == str(curr_date)) &
        (scoreboard['PLAYER_ID'] == player_id)
    ]['GAME_ID'].iloc[0]

    # finds player name
    player_name = scoreboard[scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]

    # opposition_id for the current matchup of curr_date
    opposition_id = scoreboard[scoreboard['PLAYER_ID'] == player_id]['opposition_team_id'].iloc[0]

    # finds player games against opp before current date
    player_game_logs_before_curr_date_vs_opp = curr_player_game_logs[
        (curr_player_game_logs['GAME_DATE'] < curr_date) & # before curr date
        (curr_player_game_logs['OPPOSITION_ID'] == opposition_id) & # if the opposition id is the same as the scoreboard's
        (curr_player_game_logs['GAME_DATE'] > two_years_from_curr_date) & # needs to be later than two years ago
        (curr_player_game_logs['MIN'] > 0) # needs to play more than 0 minutes
    ]

    # finds total number of games against opp
    opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

    # finds player games overall during the season
    player_game_logs_before_curr_date_overall = curr_player_game_logs[
        (curr_player_game_logs['GAME_DATE'] < curr_date) & # before curr date
        (curr_player_game_logs['GAME_DATE'] > season_start_date) & # needs to be during this season
        (curr_player_game_logs['MIN'] > 0) # needs to player more than 0 minutes
    ]

    features_list = []
    
    for prop in props:
        
        current_player_prop_row = system[
            (system['PLAYER_ID'] == player_id) &
            (system['PROP'] == translation[prop])
        ]

        if current_player_prop_row.empty:

            continue

        prop_line = current_player_prop_row['LINE'].iloc[0]
        result = current_player_prop_row['RESULT'].iloc[0]

        # defensive rank based on team and statline
        def_rank = find_defensive_rank(conn, player_positions, opposition_id, prop)

        overall_games_for_current_prop = []

        if player_game_logs_before_curr_date_overall.empty:

            continue
        
        overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()
        
        # fills in the rest of the games for the 20 games average

        if len(overall_games_for_current_prop) == 0:

            continue

        if len(overall_games_for_current_prop) < 20:

            average = sum(overall_games_for_current_prop) / len(overall_games_for_current_prop)

            while len(overall_games_for_current_prop) < 20:

                overall_games_for_current_prop.append(average)

        opp_games_for_current_prop = player_game_logs_before_curr_date_vs_opp[prop].to_list()

        average_overall_last_20 = sum(overall_games_for_current_prop) / 20

        if len(opp_games_for_current_prop) == 0:

            for i in range(10):

                opp_games_for_current_prop.append(average_overall_last_20)
        
        elif len(opp_games_for_current_prop) < 10:

            average = sum(opp_games_for_current_prop) / len(opp_games_for_current_prop)

            while len(opp_games_for_current_prop) < 10:

                opp_games_for_current_prop.append(average)

        average_opp_last_10 = sum(opp_games_for_current_prop) / 10

        last_5_games_overall = []
        last_5_games_vs_opp = []

        for i in range(min(5, len(overall_games_for_current_prop))):

            last_5_games_overall.append(overall_games_for_current_prop[i])
        
        for i in range(min(5, len(opp_games_for_current_prop))):

            last_5_games_vs_opp.append(opp_games_for_current_prop[i])
        
        if len(last_5_games_overall) < 5:

            average = sum(last_5_games_overall) / len(last_5_games_overall)

            while len(last_5_games_overall) < 5:

                last_5_games_overall.append(average)
        
        if len(last_5_games_vs_opp) < 5:

            average = sum(last_5_games_vs_opp) / len(last_5_games_vs_opp)

            while len(last_5_games_vs_opp) < 5:

                last_5_games_vs_opp.append(average)

        opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

        features_dict = {
            
            prop: {
                'PROP_LINE': float(prop_line),
                'LAST_GAME': float(last_5_games_overall[0] - prop_line),
                'SECOND_LAST_GAME': float(last_5_games_overall[1] - prop_line),
                'THIRD_LAST_GAME': float(last_5_games_overall[2] - prop_line),
                'FOURTH_LAST_GAME': float(last_5_games_overall[3] - prop_line),
                'FIFTH_LAST_GAME': float(last_5_games_overall[4] - prop_line),
                'AVERAGE_LAST_20': float(average_overall_last_20 - prop_line),
                'LAST_GAME_VS_OPP': float(last_5_games_vs_opp[0] - prop_line),
                'SECOND_LAST_GAME_VS_OPP': float(last_5_games_vs_opp[1] - prop_line),
                'THIRD_LAST_GAME_VS_OPP': float(last_5_games_vs_opp[2] - prop_line),
                'FOURTH_LAST_GAME_VS_OPP': float(last_5_games_vs_opp[3] - prop_line),
                'FIFTH_LAST_GAME_VS_OPP': float(last_5_games_vs_opp[4] - prop_line),
                'AVERAGE_LAST_10_VS_OPP': float(average_opp_last_10 - prop_line),
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
            }
        }

        features_list.append(features_dict)
    
    return features_list

if __name__ == "__main__":

    class Node:

        def __init__(self, val):

            self.val = val
            self.next = None

    class LinkedList():

        def __init__(self):

            self.head = None
        
        def appendNode(self, node_val):

            node = Node(node_val)

            prev = None
            curr = self.head

            while curr:
                
                prev = curr
                curr = curr.next
            
            if not prev:

                self.head = node
            
            else:

                prev.next = node

        def insert_reverse_sorted(self, player_name, prop, line, prob, curr_date, player_id, matchup):
            
            now = time.strftime(f"%Y-%m-%d %H:%M:%S")

            if prob < 0.5:
                
                score = float(1 - prob) * 100
                over_under = "U"
            
            elif prob > 0.5:
                
                score = float(prob * 100)
                over_under = "O"
            
            else:
                
                score = float(prob * 100)
                over_under = "O/U"

            reverse_translation = {
                "PTS": "player_points",
                "REB": "player_rebounds",
                "AST": "player_assists",
                "FG3M": "player_threes",
                'STL': 'player_steals',
                'BLK': 'player_blocks',
                "PRA": "player_points_rebounds_assists",
                "PTS_REB": "player_points_rebounds",
                "PTS_AST": "player_points_assists",
                "REB_AST": "player_rebounds_assists"
            }

            node = Node({
                'DATE': curr_date,
                "PLAYER": player_name, 
                "OVER_UNDER": over_under,
                "PROP": reverse_translation[prop], 
                "LINE": line,
                "MATCHUP": matchup,
                "SCORE": score,
                "PLAYER_ID": player_id,
                'LAST_UPDATED': now
                })

            prev = None
            curr = self.head

            while curr and (curr.val['SCORE'] > node.val['SCORE']):

                prev = curr
                curr = curr.next
            
            if not prev:
                
                node.next = self.head
                self.head = node

            else:

                prev.next = node
                node.next = curr

        def to_array(self):

            arr = []

            if not self.head:

                return arr

            curr = self.head

            while curr:

                arr.append(curr.val)
                curr = curr.next
            
            return arr

        def __repr__(self):
            
            if not self.head:

                return ""
            
            string = []

            string.append("[")

            curr = self.head

            while curr:

                string.append(f"{curr.val}")

                if not curr.next:

                    string.append("]")
                
                else:

                    string.append(", ")

                curr = curr.next
            
            return "".join(string)

    import sys
    sys.path.append(r"C:\Users\noahs\.vscode\basketball stats bot\logistic_regression")
    import joblib

    conn = sqlite3.connect(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")

    curr_date = "2025-12-20"
    season_start_date = "2025-10-21"

    game_logs = pd.read_sql_query("SELECT * FROM PLAYER_GAME_LOGS WHERE GAME_DATE <= ? ORDER BY GAME_DATE DESC", conn, params=(str(curr_date),))
    scoreboard = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE date = ?", conn, params=(str(curr_date),))
    player_positions_df = pd.read_sql_query("SELECT * FROM PLAYER_POSITIONS", conn)
    system = pd.read_sql_query("SELECT * FROM SYSTEM WHERE DATE = ?", conn, params=(str(curr_date),))

    player_ids = scoreboard.drop_duplicates('PLAYER_ID')['PLAYER_ID'].to_list()

    today_prop_features = {}

    system_list = LinkedList()

    for player_id in player_ids:

        player_name = game_logs[game_logs['PLAYER_ID'] == player_id]['PLAYER_NAME'].iloc[0]

        print(f"Finding props for {player_name}...")
        
        today_prop_features[player_id] = {'player_name': player_name, 'features': find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, system)}

    for player_id, values in today_prop_features.items():

        matchup = scoreboard[scoreboard['PLAYER_ID'] == player_id]['MATCHUP'].iloc[0]
        player_name = values['player_name']

        print(f"Adding {player_name} ({player_id}) to the system...")

        for hashmap in values['features']:

            for prop, feature in hashmap.items():

                df = pd.DataFrame([feature])

                prop_line = feature['PROP_LINE']

                models_path = r"C:\Users\noahs\.vscode\basketball stats bot\main\training\models\XGBoost"

                model_file_path = os.path.join(models_path, f"{prop}_xgboost_model.pkl")
                model = joblib.load(model_file_path)

                prob_over = model.predict_proba(df)[0][1]
                # print(f"P(OVER) for {player_name} {prop} {prop_line} =", prob_over)

                system_list.insert_reverse_sorted(player_name, prop, prop_line, prob_over, curr_date, player_id, matchup)

                arr = system_list.to_array()

    with open(f"scoringv5_{curr_date}.json", "w") as f:

        json.dump(arr, f, indent=4)


