import sqlite3
from datetime import date, timedelta, datetime
import pandas as pd
import joblib
import os
import sys
import numpy as np
import statistics

from basketball_stats_bot.config import load_config
config = load_config()


# first scoring that uses hand picked weights using the df itself and lengths
def scoringv1(game_logs, current_opposition_id, prop, line):

    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()

    date_time = date.today()

    two_years_from_curr_date = date_time - timedelta(days=730)

    against_team = game_logs[

        (game_logs['OPPOSITION_ID'] == current_opposition_id) &
        (game_logs['GAME_DATE'] > str(two_years_from_curr_date))
        
        ]

    recent_20_games = game_logs[game_logs['OPPOSITION_ID'] != current_opposition_id]

    last_five_against_team = against_team.iloc[:5]

    if len(against_team) > 5:

        rest_against_team = against_team.iloc[5:]
    
    last_tenth_game = recent_20_games['GAME_DATE'].iloc[:10].iloc[-1]

    recent_5_games = recent_20_games.iloc[5:]

    curr_score = 0

    if len(against_team) > 0:

        # when the player goes over the line in last 5 games against team and it's in the most recent 10 games
        curr_score += (len(last_five_against_team[

            (last_five_against_team['GAME_DATE'] > last_tenth_game) &
            (last_five_against_team[prop] > line)]) * 8)
        
        # when the player goes under the line in last 5 games against team and it's in the most recent 10 games
        curr_score -= (len(last_five_against_team[

            (last_five_against_team['GAME_DATE'] > last_tenth_game) &
            (last_five_against_team[prop] < line)]) * 8)
        
        # when the player goes over the line in last 5 against team but not in most recent 10 games
        curr_score += (len(last_five_against_team[

            (last_five_against_team['GAME_DATE'] < last_tenth_game) &
            (last_five_against_team[prop] > line)]) * 4)

        # when the player goes under the line in last 5 against team but not in most recent 10 games
        curr_score -= (len(last_five_against_team[

            (last_five_against_team['GAME_DATE'] < last_tenth_game) &
            (last_five_against_team[prop] < line)]) * 4)
    
    if len(against_team) > 5:

        # when the player goes over the line in last _ against team and in the 10 most recent games
        curr_score += (len(rest_against_team[
            (rest_against_team['GAME_DATE'] > last_tenth_game) &
            (rest_against_team[prop] > line)]) * 3)
        
        # when the player goes under the line in last _ against team and in the 10 most recent games
        curr_score -= (len(rest_against_team[
            (rest_against_team['GAME_DATE'] > last_tenth_game) &
            (rest_against_team[prop] < line)]) * 3)

        # when the player goes over the line in last _ against team but not in the 10 most recent games
        curr_score += (len(rest_against_team[
            (rest_against_team['GAME_DATE'] < last_tenth_game) &
            (rest_against_team[prop] > line)]) * 1.5)
        
        # when the player goes under the line in last _ against team but not in the 10 most recent games
        curr_score -= (len(rest_against_team[
            (rest_against_team['GAME_DATE'] < last_tenth_game) &
            (rest_against_team[prop] < line)]) * 1.5)

    if len(recent_20_games) > 0:
        
        # when the player goes over the line in 5 most recent games not including against team
        curr_score += (len(recent_5_games[
            (recent_5_games[prop] > line)]) * 6.5)
        
        # when the player goes under the line in 5 most recent games not including against team
        curr_score -= (len(recent_5_games[
            (recent_5_games[prop] < line)]) * 6.5)


    if len(recent_20_games) > 5:

        rest_of_games = recent_20_games.iloc[5:]
        
        # when the player goes over the line in 20 most recent games not including against team
        curr_score += (len(rest_of_games[
            (rest_of_games[prop] > line)]) * 1.5)
        
        # when the player goes under the line in 20 most recent games not including against team
        curr_score -= (len(rest_of_games[
            (rest_of_games[prop] < line)]) * 1.5)
    
    return curr_score

# hand picked weights but different
def scoringv2(game_logs, current_opposition_id, prop, line):

    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()

    date_time = date.today()

    two_years_from_curr_date = date_time - timedelta(days=730)

    against_team = game_logs[

        (game_logs['OPPOSITION_ID'] == current_opposition_id) &
        (game_logs['GAME_DATE'] > str(two_years_from_curr_date))
        
        ]
    
    n = len(against_team)

    recent_20_games = game_logs[game_logs['OPPOSITION_ID'] != current_opposition_id]

    last_tenth_game_date = recent_20_games['GAME_DATE'].iloc[:10].iloc[-1]

    curr_score = 0
    
    if n > 0:

        multiplier = 11

        for i in range(n):
            
            if against_team.iloc[i]['GAME_DATE'] > last_tenth_game_date and against_team.iloc[i][prop] > line:

                curr_score += (multiplier + 5)
            
            elif against_team.iloc[i]['GAME_DATE'] > last_tenth_game_date and against_team.iloc[i][prop] < line:

                curr_score -= (multiplier + 5)
            
            elif against_team.iloc[i]['GAME_DATE'] < last_tenth_game_date and against_team.iloc[i][prop] > line:

                curr_score += multiplier
            
            elif against_team.iloc[i]['GAME_DATE'] < last_tenth_game_date and against_team.iloc[i][prop] < line:

                curr_score -= multiplier
            
            if multiplier == 2:

                continue
            
            else:

                multiplier -= 1.5
    
    n = len(recent_20_games)

    if n > 0:

        multiplier = 10
        for i in range(n):

            if i < 5:

                if recent_20_games.iloc[i][prop] > line:

                    curr_score += (multiplier + 5)
                
                elif recent_20_games.iloc[i][prop] < line:

                    curr_score -= (multiplier + 5)
            
            else:

                if recent_20_games.iloc[i][prop] > line:

                    curr_score += multiplier
                
                elif recent_20_games.iloc[i][prop] < line:

                    curr_score -= multiplier
            
            if multiplier == 2:

                continue

            else:

                multiplier -= 2
    
    return curr_score

# hand picked weights + defensive rank
def scoringv3(game_logs, current_opposition_id, prop, line):

    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()

    date_time = date.today()

    two_years_from_curr_date = date_time - timedelta(days=730)

    against_team = game_logs[

        (game_logs['OPPOSITION_ID'] == current_opposition_id) &
        (game_logs['GAME_DATE'] > str(two_years_from_curr_date))
        
        ]
    
    n = len(against_team)

    recent_20_games = game_logs[game_logs['OPPOSITION_ID'] != current_opposition_id]

    player_id = recent_20_games['PLAYER_ID'].iloc[0]
    player_name = recent_20_games['PLAYER_NAME'].iloc[0]

    positions = pd.read_sql_query('SELECT * FROM PLAYER_POSITIONS WHERE PLAYER_ID = ?', conn, params=(int(player_id),))['POSITION'].to_list()

    last_tenth_game_date = recent_20_games['GAME_DATE'].iloc[:10].iloc[-1]

    curr_score = 0

    for position in positions:
        
        curr_df = pd.read_sql_query(f"""
                                    
                                    SELECT * FROM DEFENSE_VS_POSITION_2025_2026
                                    WHERE POSITION = ? 
                                    ORDER BY {prop} DESC
                                    
                                    """, conn, params=(position,))
        
        if curr_df.empty:

            print(f"Could not find a position for: {player_name}")

        curr_rank = curr_df.index[curr_df['TEAM_ID'] == current_opposition_id][0]

        if curr_rank > 20:

            curr_score -= (15 - (30-curr_rank) * 1.5)
        
        elif curr_rank < 10:

            curr_score += 15 - 1.5*curr_rank
        
    curr_score /= len(positions)
    
    if n > 0:

        multiplier = 11

        for i in range(n):
            
            if against_team.iloc[i]['GAME_DATE'] > last_tenth_game_date and against_team.iloc[i][prop] > line:

                curr_score += (multiplier + 5)
            
            elif against_team.iloc[i]['GAME_DATE'] > last_tenth_game_date and against_team.iloc[i][prop] < line:

                curr_score -= (multiplier + 5)
            
            elif against_team.iloc[i]['GAME_DATE'] < last_tenth_game_date and against_team.iloc[i][prop] > line:

                curr_score += multiplier
            
            elif against_team.iloc[i]['GAME_DATE'] < last_tenth_game_date and against_team.iloc[i][prop] < line:

                curr_score -= multiplier
            
            if multiplier == 2:

                continue
            
            else:

                multiplier -= 1.5
    
    n = len(recent_20_games)

    if n > 0:

        multiplier = 15
        for i in range(n):

            if i < 5:

                if recent_20_games.iloc[i][prop] > line:

                    curr_score += (multiplier + 5)
                
                elif recent_20_games.iloc[i][prop] < line:

                    curr_score -= (multiplier + 5)
            
            else:

                if recent_20_games.iloc[i][prop] > line:

                    curr_score += multiplier
                
                elif recent_20_games.iloc[i][prop] < line:

                    curr_score -= multiplier
            
            if multiplier == 2:

                continue

            else:

                multiplier -= 2
    
    return curr_score

# logistical regression model (deleted training_table_without_nans so this won't work)
def scoringv4(game_logs, current_opposition_id, prop, line, scoreboard, player_positions, curr_date):

    def find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions_df):

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
        
        today = date.today()
        two_years_from_curr_date = str(today - timedelta(days=730))

        curr_player_game_logs = game_logs[
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] > str(two_years_from_curr_date))
        ]

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

        # defensive rank based on team and statline
        def_rank = find_defensive_rank(conn, player_positions, current_opposition_id, prop)

        overall_games_for_current_prop = []

        if player_game_logs_before_curr_date_overall.empty:

            return -1
        
        overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()
        
        # fills in the rest of the games for the 20 games average

        if len(overall_games_for_current_prop) == 0:

            return -1

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
                'PROP_LINE': float(line),
                'LAST_GAME': last_5_games_overall[0] - line,
                'SECOND_LAST_GAME': last_5_games_overall[1] - line,
                'THIRD_LAST_GAME': last_5_games_overall[2] - line,
                'FOURTH_LAST_GAME': last_5_games_overall[3] - line,
                'FIFTH_LAST_GAME': last_5_games_overall[4] - line,
                'AVERAGE_LAST_20': average_overall_last_20 - line,
                'LAST_GAME_VS_OPP': last_5_games_vs_opp[0] - line,
                'SECOND_LAST_GAME_VS_OPP': last_5_games_vs_opp[1] - line,
                'THIRD_LAST_GAME_VS_OPP': last_5_games_vs_opp[2] - line,
                'FOURTH_LAST_GAME_VS_OPP': last_5_games_vs_opp[3] - line,
                'FIFTH_LAST_GAME_VS_OPP': last_5_games_vs_opp[4] - line,
                'AVERAGE_LAST_10_VS_OPP': average_opp_last_10 - line,
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
            }
        }
    
        return features_dict

    conn = sqlite3.connect(config.DB_PATH)
    season_start_date = '2025-10-21'

    player_name = game_logs['PLAYER_NAME'].iloc[0]
    player_id = game_logs['PLAYER_ID'].iloc[0]

    today_prop_features = {}

    today_prop_features[player_id] = {'player_name': player_name, 'features': find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions)}

    for player_id, values in today_prop_features.items():

        matchup = scoreboard[scoreboard['PLAYER_ID'] == player_id]['MATCHUP'].iloc[0]
        player_name = values['player_name']

        hashmap = values['features']

        if hashmap == -1:

            return -2
        
        for prop, feature in hashmap.items():

            df = pd.DataFrame([feature])

            scaler_file_path = os.path.join(config.LOG_REG_PATH, f"{prop}_logreg_scaler.pkl")
            model_file_path = os.path.join(config.LOG_REG_PATH, f"{prop}_logreg_model.pkl")
            scaler = joblib.load(scaler_file_path)
            model = joblib.load(model_file_path)

            today_scaled = scaler.transform(df)

            prob_over = model.predict_proba(today_scaled)[0][1] * 100

        if prob_over < 50:

            return 0 - (100 - prob_over)
        
        elif prob_over >= 50:

            return prob_over

# xgboost model with padded averages (deleted training table without nans so this won't work)
def scoringv5(game_logs, current_opposition_id, prop, line, scoreboard, player_positions, curr_date):

    def find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions_df):

        def find_defensive_rank(conn, positions, team_id, prop):

            rank = 0

            for position in positions:

                df = pd.read_sql_query(f"SELECT * FROM DEFENSE_VS_POSITION_2025_2026 WHERE POSITION = ? ORDER BY {prop} DESC", conn, params=(position,))

                rank += df.index[df['TEAM_ID'] == team_id][0]
            
            return rank / len(positions)
        
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
        
        today = datetime.strptime(str(curr_date), "%Y-%m-%d").date()
        two_years_from_curr_date = str(today - timedelta(days=730))

        curr_player_game_logs = game_logs[
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] > str(two_years_from_curr_date))
        ]

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

        # defensive rank based on team and statline
        player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

        def_rank = find_defensive_rank(conn, player_positions, current_opposition_id, prop)

        overall_games_for_current_prop = []

        if player_game_logs_before_curr_date_overall.empty:

            print(f"Couldn't find player game logs before the overall date for {player_name}")
            return -2
        
        overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()
        
        # fills in the rest of the games for the 20 games average

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
                'PROP_LINE': float(line),
                'LAST_GAME': last_5_games_overall[0] - line,
                'SECOND_LAST_GAME': last_5_games_overall[1] - line,
                'THIRD_LAST_GAME': last_5_games_overall[2] - line,
                'FOURTH_LAST_GAME': last_5_games_overall[3] - line,
                'FIFTH_LAST_GAME': last_5_games_overall[4] - line,
                'AVERAGE_LAST_20': average_overall_last_20 - line,
                'LAST_GAME_VS_OPP': last_5_games_vs_opp[0] - line,
                'SECOND_LAST_GAME_VS_OPP': last_5_games_vs_opp[1] - line,
                'THIRD_LAST_GAME_VS_OPP': last_5_games_vs_opp[2] - line,
                'FOURTH_LAST_GAME_VS_OPP': last_5_games_vs_opp[3] - line,
                'FIFTH_LAST_GAME_VS_OPP': last_5_games_vs_opp[4] - line,
                'AVERAGE_LAST_10_VS_OPP': average_opp_last_10 - line,
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
            }
        }
    
        return features_dict

    conn = sqlite3.connect(config.DB_PATH)
    season_start_date = '2025-10-21'

    today = datetime.strptime(curr_date, "%Y-%m-%d").date()
    two_years_from_curr_date = str(today - timedelta(days=730))

    player_id = game_logs['PLAYER_ID'].iloc[0]
    player_name = game_logs['NAME_CLEAN'].iloc[0]

    today_prop_features = {}

    today_prop_features[player_id] = {'player_name': player_name, 'features': find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions)}

    for player_id, values in today_prop_features.items():

        matchup = scoreboard[scoreboard['PLAYER_ID'] == player_id]['MATCHUP'].iloc[0]
        player_name = values['player_name']

        hashmap = values['features']

        if hashmap == -2:

            continue

        for prop, feature in hashmap.items():
            
            df = pd.DataFrame([feature])

            model_file_path = os.path.join(config.XGBOOST_PATH, f"{prop}_xgboost_model.pkl")
            model = joblib.load(model_file_path)

            prob_over = model.predict_proba(df)[0][1] * 100

        if prob_over < 50:

            return 0 - (100 - prob_over)
        
        elif prob_over >= 50:

            return prob_over

# base xgboost model with NAN values
def scoringv6(game_logs, current_opposition_id, prop, line, scoreboard, player_positions, curr_date):

    def find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions_df):

        def find_defensive_rank(conn, positions, team_id, prop):

            rank = 0

            for position in positions:

                df = pd.read_sql_query(f"SELECT * FROM DEFENSE_VS_POSITION_2025_2026 WHERE POSITION = ? ORDER BY {prop} DESC", conn, params=(position,))

                rank += df.index[df['TEAM_ID'] == team_id][0]
            
            return rank / len(positions)
        
        def find_overall_games(curr_player_game_logs, prop_line):

            player_game_logs_before_curr_date_overall = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] < curr_date) & # before curr date
                (curr_player_game_logs['GAME_DATE'] > season_start_date) & # needs to be during this season
                (curr_player_game_logs['MIN'] > 0) # needs to player more than 0 minutes
            ]

            if player_game_logs_before_curr_date_overall.empty:

                print(f"Couldn't find player game logs before the overall date for {player_name}")
                return -2

            overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()
        
            if not overall_games_for_current_prop:

                return np.nan, np.nan

            average_overall_last_20 = sum(overall_games_for_current_prop) / len(overall_games_for_current_prop) - prop_line

            last_5_games_overall = []

            for i in range(min(5, len(overall_games_for_current_prop))):

                last_5_games_overall.append(overall_games_for_current_prop[i])

            return last_5_games_overall, average_overall_last_20

        def find_opp_games(curr_player_game_logs):

            player_game_logs_before_curr_date_vs_opp = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] < curr_date) & # before curr date
                (curr_player_game_logs['OPPOSITION_ID'] == opposition_id) & # if the opposition id is the same as the scoreboard's
                (curr_player_game_logs['GAME_DATE'] > two_years_from_curr_date) & # needs to be later than two years ago
                (curr_player_game_logs['MIN'] > 0) # needs to play more than 0 minutes
            ]

            opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

            opp_games_for_current_prop = player_game_logs_before_curr_date_vs_opp[prop].to_list()

            if len(opp_games_for_current_prop) == 0:

                for i in range(10):

                    opp_games_for_current_prop.append(None)
            
            elif len(opp_games_for_current_prop) < 10:

                while len(opp_games_for_current_prop) < 10:

                    opp_games_for_current_prop.append(None)
            
            for i in range(10):
            
                if opp_games_for_current_prop[i] != None:

                    average_opp_last_10 += opp_games_for_current_prop[i]
                    games_played += 1
            
            if games_played > 0:

                    average_opp_last_10 = average_opp_last_10 / games_played - line
            
            else:
                
                average_opp_last_10 = np.nan

            last_5_games_vs_opp = []
            
            for i in range(min(5, len(opp_games_for_current_prop))):

                last_5_games_vs_opp.append(opp_games_for_current_prop[i])
            
            return last_5_games_vs_opp, average_opp_last_10, opp_game_count


        today = datetime.strptime(str(curr_date), "%Y-%m-%d").date()
        two_years_from_curr_date = str(today - timedelta(days=730))

        curr_player_game_logs = game_logs[
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] > str(two_years_from_curr_date))
        ]

        player_name = scoreboard[scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]
        opposition_id = scoreboard[scoreboard['PLAYER_ID'] == player_id]['opposition_team_id'].iloc[0]
        player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

        def_rank = find_defensive_rank(conn, player_positions, current_opposition_id, prop)
        last_5_games_overall, average_overall_last_20 = find_overall_games(curr_player_game_logs=curr_player_game_logs, prop_line=line)
        last_5_games_vs_opp, average_opp_last_10, opp_game_count = find_opp_games(curr_player_game_logs=curr_player_game_logs)
        
        last_game = (
            last_5_games_overall[0] - line
            if len(last_5_games_overall) > 0
            else np.nan
        )
        second_last_game = (
            last_5_games_overall[1] - line
            if len(last_5_games_overall) > 1
            else np.nan
        )
        third_last_game = (
            last_5_games_overall[2] - line
            if len(last_5_games_overall) > 2
            else np.nan
        )
        fourth_last_game = (
            last_5_games_overall[3] - line
            if len(last_5_games_overall) > 3
            else np.nan
        )
        fifth_last_game = (
            last_5_games_overall[4] - line
            if len(last_5_games_overall) > 4
            else np.nan
        )
        last_game_vs_opp = (
            last_5_games_vs_opp[0] - line
            if last_5_games_vs_opp[0] != None
            else np.nan
        )
        second_last_game_vs_opp = (
            last_5_games_vs_opp[1] - line
            if last_5_games_vs_opp[1] != None
            else np.nan
        )
        third_last_game_vs_opp = (
            last_5_games_vs_opp[2] - line
            if last_5_games_vs_opp[2] != None
            else np.nan
        )
        fourth_last_game_vs_opp = (
            last_5_games_vs_opp[3] - line
            if last_5_games_vs_opp[3] != None
            else np.nan
        )
        fifth_last_game_vs_opp = (
            last_5_games_vs_opp[4] - line
            if last_5_games_vs_opp[4] != None
            else np.nan
        )

        features_dict = {
            
            prop: {
                'LAST_GAME': last_game,
                'SECOND_LAST_GAME': second_last_game,
                'THIRD_LAST_GAME': third_last_game,
                'FOURTH_LAST_GAME': fourth_last_game,
                'FIFTH_LAST_GAME': fifth_last_game,
                'AVERAGE_LAST_20': average_overall_last_20,
                'LAST_GAME_VS_OPP': last_game_vs_opp,
                'SECOND_LAST_GAME_VS_OPP': second_last_game_vs_opp,
                'THIRD_LAST_GAME_VS_OPP': third_last_game_vs_opp,
                'FOURTH_LAST_GAME_VS_OPP': fourth_last_game_vs_opp,
                'FIFTH_LAST_GAME_VS_OPP': fifth_last_game_vs_opp,
                'AVERAGE_LAST_10_VS_OPP': average_opp_last_10,
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
            }
        }
    
        return features_dict

    conn = sqlite3.connect(config.DB_PATH)
    season_start_date = '2025-10-21'

    player_id = game_logs['PLAYER_ID'].iloc[0]
    player_name = game_logs['NAME_CLEAN'].iloc[0]

    today_features = {}

    today_features[player_id] = {'player_name': player_name, 'features': find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions)}

    for player_id, values in today_features.items():

        player_name = values['player_name']

        hashmap = values['features']

        if hashmap == -2:

            continue

        for prop, feature in hashmap.items():
            
            df = pd.DataFrame([feature])

            model_file_path = os.path.join(config.XGBOOST_PATH, f"{prop}_xgboost_model_with_NANs.pkl")
            model = joblib.load(model_file_path)

            prob_over = model.predict_proba(df)[0][1] * 100

        if prob_over < 50:

            return 0 - (100 - prob_over)
        
        elif prob_over >= 50:

            return prob_over

# xgboost model with usage percentages
def scoringv7(game_logs, current_opposition_id, prop, line, scoreboard, player_positions, curr_date, team_totals_df, minutes_projection, season_game_logs):

    def find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions_df, team_totals_df, minutes_projection, season_game_logs):

        def find_team_totals_and_player_share(curr_game_logs, stat, team_totals, team_id):
            
            if curr_game_logs.empty:

                return np.nan, np.nan, np.nan, np.nan

            col_name = f"PCT_{stat}_USAGE"

            curr_game_logs = curr_game_logs.sort_values("GAME_DATE", ascending=False)

            last_5_player_games = curr_game_logs.iloc[:5]
            last_10_player_games = curr_game_logs.iloc[:10]

            curr_team_totals_df_list = []

            all_team_ids = curr_game_logs.drop_duplicates("TEAM_ID")['TEAM_ID'].to_list()

            curr_team_totals_df_list = []

            for game_id in last_10_player_games['GAME_ID'].to_list():
                
                if isinstance(game_id, int):
                    
                    game_id = "00" + str(game_id)

                curr_game_team_total = team_totals[
                    (team_totals['GAME_ID'] == game_id) &
                    (team_totals['TEAM_ID'] == str(team_id))
                ]

                if curr_game_team_total.empty:
                    
                    for curr_team_id in all_team_ids:

                        curr_game_team_total = team_totals[
                            (team_totals['GAME_ID'] == game_id) &
                            (team_totals['TEAM_ID'] == str(curr_team_id))
                        ]

                        if not curr_game_team_total.empty:

                            break
                
                if curr_game_team_total.empty:
                    print(f"Current Game ID Team Totals are empty..")
                    print(game_id, all_team_ids)
                    sys.exit(1)

                curr_team_totals_df_list.append(curr_game_team_total)

            if not curr_team_totals_df_list:

                print(f"Could not find team totals for {game_id} and team: {team_id}")
                sys.exit(1)
                
            curr_team_totals_df = pd.concat(curr_team_totals_df_list, ignore_index=True)

            avg_last_5_pct_share = float(last_5_player_games[col_name].sum()) / len(last_5_player_games[col_name])
            avg_last_10_pct_share = float(last_10_player_games[col_name].sum()) / len(last_10_player_games[col_name])

            team_totals_last_5 = curr_team_totals_df.iloc[:5]
            team_totals_last_10 = curr_team_totals_df.iloc[:10]

            avg_last_5_team_totals = float(team_totals_last_5[stat].sum()) / len(team_totals_last_5)
            avg_last_10_team_totals = float(team_totals_last_10[stat].sum()) / len(team_totals_last_10)

            return avg_last_5_pct_share, avg_last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals

        def find_defensive_rank(conn, positions, team_id, prop):

            rank = 0

            for position in positions:

                df = pd.read_sql_query(f"SELECT * FROM DEFENSE_VS_POSITION_2025_2026 WHERE POSITION = ? ORDER BY {prop} DESC", conn, params=(position,))

                rank += df.index[df['TEAM_ID'] == team_id][0]
            
            return rank / len(positions)
        
        def find_overall_games(curr_player_game_logs, prop_line, player_name):

            player_game_logs_before_curr_date_overall = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] < curr_date) & # before curr date
                (curr_player_game_logs['GAME_DATE'] >= season_start_date) & # needs to be during this season
                (curr_player_game_logs['MIN'] > 0) # needs to player more than 0 minutes
            ]

            if player_game_logs_before_curr_date_overall.empty:

                print(f"Couldn't find player game logs before the overall date for {player_name}")

                return [], np.nan

            overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()
        
            if not overall_games_for_current_prop:

                return np.nan, np.nan

            average_overall_last_20 = sum(overall_games_for_current_prop) / len(overall_games_for_current_prop) - prop_line

            last_5_games_overall = []

            for i in range(min(5, len(overall_games_for_current_prop))):

                last_5_games_overall.append(overall_games_for_current_prop[i])

            return last_5_games_overall, average_overall_last_20

        def find_opp_games(curr_player_game_logs, prop_line):

            player_game_logs_before_curr_date_vs_opp = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] < curr_date) & # before curr date
                (curr_player_game_logs['OPPOSITION_ID'] == current_opposition_id) & # if the opposition id is the same as the scoreboard's
                (curr_player_game_logs['GAME_DATE'] >= two_years_from_curr_date) & # needs to be later than two years ago
                (curr_player_game_logs['MIN'] > 0) # needs to play more than 0 minutes
            ]

            opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

            opp_games_for_current_prop = player_game_logs_before_curr_date_vs_opp[prop].to_list()

            if len(opp_games_for_current_prop) == 0:

                for i in range(10):

                    opp_games_for_current_prop.append(None)
            
            elif len(opp_games_for_current_prop) < 10:

                while len(opp_games_for_current_prop) < 10:

                    opp_games_for_current_prop.append(None)
                
            average_opp_last_10 = 0
            games_played = 0
            
            for i in range(10):
            
                if opp_games_for_current_prop[i] != None:

                    average_opp_last_10 += opp_games_for_current_prop[i]
                    games_played += 1
            
            if games_played > 0:

                    average_opp_last_10 = average_opp_last_10 / games_played - prop_line
            
            else:
                
                average_opp_last_10 = np.nan

            last_5_games_vs_opp = []
            
            for i in range(min(5, len(opp_games_for_current_prop))):

                last_5_games_vs_opp.append(opp_games_for_current_prop[i])
            
            return last_5_games_vs_opp, average_opp_last_10, opp_game_count

        today = datetime.strptime(str(curr_date), "%Y-%m-%d").date()
        two_years_from_curr_date = str(today - timedelta(days=730))

        curr_player_game_logs = game_logs[
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] >= str(two_years_from_curr_date)) &
            (game_logs['GAME_DATE'] < str(curr_date))

        ]

        curr_season_player_game_logs = season_game_logs[

            (season_game_logs['PLAYER_ID'] == player_id) &
            (season_game_logs['GAME_DATE'] < str(curr_date))

        ]

        if curr_season_player_game_logs.empty:

            avg_last_5_pct_share = np.nan
            avg_last_10_pct_share = np.nan
            avg_last_5_team_totals = np.nan
            avg_last_10_team_totals = np.nan
            avg_last_5_pct_share = np.nan
        
        else:


            curr_team_id = curr_season_player_game_logs['TEAM_ID'].iloc[0]
            avg_last_5_pct_share, avg_last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals = find_team_totals_and_player_share(curr_season_player_game_logs, prop, team_totals_df, curr_team_id)

        player_name = scoreboard[scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]
        player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()
        def_rank = find_defensive_rank(conn, player_positions, current_opposition_id, prop)
        last_5_games_overall, average_overall_last_20_minus_line = find_overall_games(curr_player_game_logs=curr_player_game_logs, prop_line=line, player_name=player_name)
        last_5_games_vs_opp, average_opp_last_10, opp_game_count = find_opp_games(curr_player_game_logs=curr_player_game_logs, prop_line=line)

        if last_5_games_overall == -2:

            return -2


        if np.isnan(minutes_projection):

            expected_from_last_5_minus_line = np.nan
            expected_from_last_10_minus_line = np.nan
        
        else:

            expected_from_last_5_minus_line = ((avg_last_5_team_totals / 240) * minutes_projection * avg_last_5_pct_share) - line
            expected_from_last_10_minus_line = ((avg_last_10_team_totals / 240) * minutes_projection * avg_last_10_pct_share) - line

        

        last_game = (
            last_5_games_overall[0] - line
            if len(last_5_games_overall) > 0
            else np.nan
        )
        second_last_game = (
            last_5_games_overall[1] - line
            if len(last_5_games_overall) > 1
            else np.nan
        )
        third_last_game = (
            last_5_games_overall[2] - line
            if len(last_5_games_overall) > 2
            else np.nan
        )
        fourth_last_game = (
            last_5_games_overall[3] - line
            if len(last_5_games_overall) > 3
            else np.nan
        )
        fifth_last_game = (
            last_5_games_overall[4] - line
            if len(last_5_games_overall) > 4
            else np.nan
        )
        last_game_vs_opp = (
            last_5_games_vs_opp[0] - line
            if last_5_games_vs_opp[0] != None
            else np.nan
        )
        second_last_game_vs_opp = (
            last_5_games_vs_opp[1] - line
            if last_5_games_vs_opp[1] != None
            else np.nan
        )
        third_last_game_vs_opp = (
            last_5_games_vs_opp[2] - line
            if last_5_games_vs_opp[2] != None
            else np.nan
        )
        fourth_last_game_vs_opp = (
            last_5_games_vs_opp[3] - line
            if last_5_games_vs_opp[3] != None
            else np.nan
        )
        fifth_last_game_vs_opp = (
            last_5_games_vs_opp[4] - line
            if last_5_games_vs_opp[4] != None
            else np.nan
        )

        features_dict = {
            
            prop: {
                'LAST_GAME': last_game,
                'SECOND_LAST_GAME': second_last_game,
                'THIRD_LAST_GAME': third_last_game,
                'FOURTH_LAST_GAME': fourth_last_game,
                'FIFTH_LAST_GAME': fifth_last_game,
                'AVERAGE_LAST_20': average_overall_last_20_minus_line,
                'LAST_GAME_VS_OPP': last_game_vs_opp,
                'SECOND_LAST_GAME_VS_OPP': second_last_game_vs_opp,
                'THIRD_LAST_GAME_VS_OPP': third_last_game_vs_opp,
                'FOURTH_LAST_GAME_VS_OPP': fourth_last_game_vs_opp,
                'FIFTH_LAST_GAME_VS_OPP': fifth_last_game_vs_opp,
                'AVERAGE_LAST_10_VS_OPP': average_opp_last_10,
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
                f'AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE': expected_from_last_5_minus_line,
                f'AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE': expected_from_last_10_minus_line
            }
        }
    
        return features_dict

    conn = sqlite3.connect(config.DB_PATH)
    season_start_date = '2025-10-21'

    game_logs = game_logs.sort_values("GAME_DATE", ascending=False)

    player_id = game_logs['PLAYER_ID'].iloc[0]
    player_name = game_logs['NAME_CLEAN'].iloc[0]

    today_features = {}

    today_features[player_id] = {'player_name': player_name, 'features': find_features(game_logs, player_id, conn, str(curr_date), scoreboard, season_start_date, player_positions, team_totals_df, minutes_projection, season_game_logs)}

    for player_id, values in today_features.items():

        player_name = values['player_name']

        hashmap = values['features']

        if hashmap == -2:

            continue

        for prop, feature in hashmap.items():
            
            df = pd.DataFrame([feature])

            model_file_path = os.path.join(config.XGBOOST_PATH, "scoringv7", f"{prop}_xgboost_model_scoring_v7.pkl")
            model = joblib.load(model_file_path)

            prob_over = model.predict_proba(df)[0][1] * 100

        if prob_over < 50:

            return 0 - (100 - prob_over)
        
        elif prob_over >= 50:

            return prob_over

# xgboost model with percentages and venue status
def scoringv8(game_logs, current_opposition_id, prop, line, scoreboard, player_positions, curr_date, team_totals_per_player_df, minutes_projection, season_game_logs):

    def find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions_df, team_totals_df, minutes_projection, season_game_logs):

        def find_team_totals_and_player_share(curr_game_logs, stat, team_totals_per_player_df):

            if curr_game_logs.empty:

                return np.nan, np.nan, np.nan, np.nan, np.nan, np.nan

            col_name = f"PCT_{stat}_USAGE"

            curr_game_logs = curr_game_logs.sort_values("GAME_DATE", ascending=False)

            last_5_player_games = curr_game_logs.iloc[:5]
            last_10_player_games = curr_game_logs.iloc[:10]

            l5_game_ids = last_5_player_games['GAME_ID'].to_list()
            l5_pct = last_5_player_games[col_name].to_list()
            l5_minutes = last_5_player_games['MIN'].to_list()
            l5_games = list(zip(l5_game_ids, l5_pct, l5_minutes))

            l10_game_ids = last_10_player_games['GAME_ID'].to_list()
            l10_pct = last_10_player_games[col_name].to_list()
            l10_minutes = last_5_player_games['MIN'].to_list()
            l10_games = list(zip(l10_game_ids, l10_pct, l10_minutes))

            corrupted_in_last_5 = set()
            corrupted_in_last_10 = set()

            for game_id in l5_game_ids:

                if game_id in config.CORRUPTED_GAME_ROTATION_GAME_IDS:

                    corrupted_in_last_5.add(game_id)

            for game_id in l10_game_ids:

                if game_id in config.CORRUPTED_GAME_ROTATION_GAME_IDS:

                    corrupted_in_last_10.add(game_id)
            
            slice_for_last_5 = len(last_5_player_games) - len(corrupted_in_last_5)
            slice_for_last_10 = len(last_10_player_games) - len(corrupted_in_last_10)

            team_totals_per_player_L5 = team_totals_per_player_df[:slice_for_last_5]
            team_totals_per_player_L10 = team_totals_per_player_df[:slice_for_last_10]

            avg_last_5_team_totals = (
                np.nan
                if len(team_totals_per_player_L5) == 0
                else team_totals_per_player_L5[stat].sum() / len(team_totals_per_player_L5)
            )
            avg_last_10_team_totals = (
                np.nan
                if len(team_totals_per_player_L10) == 0
                else team_totals_per_player_L10[stat].sum() / len(team_totals_per_player_L10)
            )

            avg_last_5_minutes = 0
            avg_last_10_minutes = 0
            avg_last_5_pct_share = 0
            avg_last_10_pct_share = 0

            for gid, curr_pct, minutes in l5_games:

                if gid in corrupted_in_last_5:

                    continue

                avg_last_5_pct_share += curr_pct
                avg_last_5_minutes += minutes
            
            for gid, curr_pct, minutes in l10_games:

                if gid in corrupted_in_last_10:

                    continue

                avg_last_10_pct_share += curr_pct
                avg_last_10_minutes += minutes
        

            avg_last_5_pct_share = (
                np.nan
                if (len(last_5_player_games) - len(corrupted_in_last_5)) == 0
                else avg_last_5_pct_share / (len(last_5_player_games) - len(corrupted_in_last_5))
            )
            avg_last_10_pct_share = (
                np.nan
                if (len(last_10_player_games) - len(corrupted_in_last_10)) == 0
                else avg_last_10_pct_share / (len(last_10_player_games) - len(corrupted_in_last_10))
            )
            avg_last_5_minutes = (
                np.nan
                if (len(last_5_player_games) - len(corrupted_in_last_5)) == 0
                else avg_last_5_minutes / (len(last_5_player_games) - len(corrupted_in_last_5))
            )
            avg_last_10_minutes = (
                np.nan
                if (len(last_10_player_games) - len(corrupted_in_last_10)) == 0
                else avg_last_10_minutes / (len(last_10_player_games) - len(corrupted_in_last_10))
            )
            
            return avg_last_5_pct_share, avg_last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals, avg_last_5_minutes, avg_last_10_minutes

        def find_defensive_rank(conn, positions, team_id, prop):

            rank = 0

            for position in positions:

                df = pd.read_sql_query(f"SELECT * FROM DEFENSE_VS_POSITION_2025_2026 WHERE POSITION = ? ORDER BY {prop} DESC", conn, params=(position,))

                rank += df.index[df['TEAM_ID'] == team_id][0]
            
            return rank / len(positions)
        
        def find_overall_games(curr_player_game_logs, prop_line):

            player_game_logs_before_curr_date_overall = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] < curr_date) & # before curr date
                (curr_player_game_logs['GAME_DATE'] > season_start_date) & # needs to be during this season
                (curr_player_game_logs['MIN'] > 0) # needs to player more than 0 minutes
            ]

            if player_game_logs_before_curr_date_overall.empty:

                print(f"Couldn't find player game logs before the overall date for {player_name}")
                return [], np.nan

            overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()
        
            if not overall_games_for_current_prop:

                return np.nan, np.nan

            curr_average_overall_last_20_minus_line= sum(overall_games_for_current_prop) / len(overall_games_for_current_prop) - prop_line

            last_5_games_overall = []

            for i in range(min(5, len(overall_games_for_current_prop))):

                last_5_games_overall.append(overall_games_for_current_prop[i])

            return last_5_games_overall, curr_average_overall_last_20_minus_line

        def find_opp_games(curr_player_game_logs):

            player_game_logs_before_curr_date_vs_opp = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] < curr_date) & # before curr date
                (curr_player_game_logs['OPPOSITION_ID'] == opposition_id) & # if the opposition id is the same as the scoreboard's
                (curr_player_game_logs['GAME_DATE'] > two_years_from_curr_date) & # needs to be later than two years ago
                (curr_player_game_logs['MIN'] > 0) # needs to play more than 0 minutes
            ]

            opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

            opp_games_for_current_prop = player_game_logs_before_curr_date_vs_opp[prop].to_list()

            if len(opp_games_for_current_prop) == 0:

                for i in range(10):

                    opp_games_for_current_prop.append(None)
            
            elif len(opp_games_for_current_prop) < 10:

                while len(opp_games_for_current_prop) < 10:

                    opp_games_for_current_prop.append(None)
                
            average_opp_last_10 = 0
            games_played = 0
            
            for i in range(10):
            
                if opp_games_for_current_prop[i] != None:

                    average_opp_last_10 += opp_games_for_current_prop[i]
                    games_played += 1
            
            if games_played > 0:

                    average_opp_last_10 = average_opp_last_10 / games_played - line
            
            else:
                
                average_opp_last_10 = np.nan

            last_5_games_vs_opp = []
            
            for i in range(min(5, len(opp_games_for_current_prop))):

                last_5_games_vs_opp.append(opp_games_for_current_prop[i])
            
            return last_5_games_vs_opp, average_opp_last_10, opp_game_count


        today = datetime.strptime(str(curr_date), "%Y-%m-%d").date()
        two_years_from_curr_date = str(today - timedelta(days=730))

        curr_player_game_logs = game_logs[
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] > str(two_years_from_curr_date))

        ]

        curr_season_player_game_logs = season_game_logs[

            (season_game_logs['PLAYER_ID'] == player_id) &
            (season_game_logs['GAME_DATE'] < str(curr_date))

        ]

        curr_player_scoreboard = scoreboard[scoreboard['PLAYER_ID'] == player_id]
        player_name = curr_player_scoreboard['PLAYER'].iloc[0]
        opposition_id = curr_player_scoreboard['opposition_team_id'].iloc[0]
        game_id = curr_player_scoreboard['GAME_ID'].iloc[0]
        venue = (
            0
            if '@' in curr_player_scoreboard['MATCHUP'].iloc[0]
            else 1
        )
        player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

        if len(player_positions) == 0:

            print(f"Could not find player positions for {player_name} ({player_id}) Check scoring_functions.py Line 1511")
            sys.exit(1)

        curr_team_total_per_player = team_totals_per_player_df[
                (team_totals_per_player_df['PLAYER_ID'] == player_id) &
                (team_totals_per_player_df['GAME_ID'] == game_id) &
                (team_totals_per_player_df['MIN'] > 0)
            ]

        curr_team_total_per_player = curr_team_total_per_player.sort_values('GAME_DATE', ascending=False)

        (avg_last_5_pct_share, avg_last_10_pct_share, 
         avg_last_5_team_totals, avg_last_10_team_totals, 
         avg_last_5_minutes, avg_last_10_minutes
        ) = find_team_totals_and_player_share(curr_season_player_game_logs, prop, curr_team_total_per_player)

        def_rank = find_defensive_rank(conn, player_positions, current_opposition_id, prop)
        last_5_games_overall, average_overall_last_20_minus_line = find_overall_games(curr_player_game_logs=curr_player_game_logs, prop_line=line)
        last_5_games_vs_opp, average_opp_last_10, opp_game_count = find_opp_games(curr_player_game_logs=curr_player_game_logs)

        if last_5_games_overall == -2:

            return -2


        if np.isnan(minutes_projection):

            expected_from_last_5_minus_line = np.nan
            expected_from_last_10_minus_line = np.nan
        
        else:

            expected_from_last_5_minus_line = (((avg_last_5_team_totals * avg_last_5_pct_share) / avg_last_5_minutes) * minutes_projection) - line
            expected_from_last_10_minus_line = (((avg_last_10_team_totals * avg_last_10_pct_share) / avg_last_10_minutes) * minutes_projection) - line

        

        last_game = (
            last_5_games_overall[0] - line
            if len(last_5_games_overall) > 0
            else np.nan
        )
        second_last_game = (
            last_5_games_overall[1] - line
            if len(last_5_games_overall) > 1
            else np.nan
        )
        third_last_game = (
            last_5_games_overall[2] - line
            if len(last_5_games_overall) > 2
            else np.nan
        )
        fourth_last_game = (
            last_5_games_overall[3] - line
            if len(last_5_games_overall) > 3
            else np.nan
        )
        fifth_last_game = (
            last_5_games_overall[4] - line
            if len(last_5_games_overall) > 4
            else np.nan
        )
        last_game_vs_opp = (
            last_5_games_vs_opp[0] - line
            if last_5_games_vs_opp[0] != None
            else np.nan
        )
        second_last_game_vs_opp = (
            last_5_games_vs_opp[1] - line
            if last_5_games_vs_opp[1] != None
            else np.nan
        )
        third_last_game_vs_opp = (
            last_5_games_vs_opp[2] - line
            if last_5_games_vs_opp[2] != None
            else np.nan
        )
        fourth_last_game_vs_opp = (
            last_5_games_vs_opp[3] - line
            if last_5_games_vs_opp[3] != None
            else np.nan
        )
        fifth_last_game_vs_opp = (
            last_5_games_vs_opp[4] - line
            if last_5_games_vs_opp[4] != None
            else np.nan
        )

        features_dict = {
            
            prop: {
                'LAST_GAME': last_game,
                'SECOND_LAST_GAME': second_last_game,
                'THIRD_LAST_GAME': third_last_game,
                'FOURTH_LAST_GAME': fourth_last_game,
                'FIFTH_LAST_GAME': fifth_last_game,
                'AVERAGE_LAST_20': average_overall_last_20_minus_line,
                'LAST_GAME_VS_OPP': last_game_vs_opp,
                'SECOND_LAST_GAME_VS_OPP': second_last_game_vs_opp,
                'THIRD_LAST_GAME_VS_OPP': third_last_game_vs_opp,
                'FOURTH_LAST_GAME_VS_OPP': fourth_last_game_vs_opp,
                'FIFTH_LAST_GAME_VS_OPP': fifth_last_game_vs_opp,
                'AVERAGE_LAST_10_VS_OPP': average_opp_last_10,
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
                f'AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE': expected_from_last_5_minus_line,
                f'AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE': expected_from_last_10_minus_line,
                'VENUE': venue
            }
        }
    
        return features_dict

    conn = sqlite3.connect(config.DB_PATH)
    season_start_date = '2025-10-21'

    game_logs = game_logs.sort_values("GAME_DATE", ascending=False)

    player_id = game_logs['PLAYER_ID'].iloc[0]
    player_name = game_logs['NAME_CLEAN'].iloc[0]

    today_features = {}

    today_features[player_id] = {'player_name': player_name, 'features': find_features(game_logs, player_id, conn, str(curr_date), scoreboard, season_start_date, player_positions, team_totals_df, minutes_projection, season_game_logs)}

    for player_id, values in today_features.items():

        player_name = values['player_name']

        hashmap = values['features']

        if hashmap == -2:

            continue

        for prop, feature in hashmap.items():
            
            df = pd.DataFrame([feature])

            model_file_path = os.path.join(config.XGBOOST_PATH, "scoringv8", f"{prop}_xgboost_model_scoring_v8.pkl")
            model = joblib.load(model_file_path)

            prob_over = model.predict_proba(df)[0][1] * 100

        if prob_over < 50:

            return 0 - (100 - prob_over)
        
        elif prob_over >= 50:

            return prob_over

# like scoringv8 but uses averages instead of singular game stats ( current best )
def scoringv9(game_logs, current_opposition_id, prop, line, scoreboard, player_positions, curr_date, team_totals_per_player_df, minutes_projection, season_game_logs, conn):

    def find_features(game_logs, player_id, conn, curr_date, scoreboard, season_start_date, player_positions_df, team_totals_per_player_df, minutes_projection, season_game_logs):

        def find_team_totals_and_player_share(curr_game_logs, stat, team_totals_per_player_df):

            if curr_game_logs.empty:

                return np.nan, np.nan, np.nan, np.nan, np.nan, np.nan

            col_name = f"PCT_{stat}_USAGE"

            curr_game_logs = curr_game_logs.sort_values("GAME_DATE", ascending=False)

            last_5_player_games = curr_game_logs.iloc[:5]
            last_10_player_games = curr_game_logs.iloc[:10]

            l5_game_ids = last_5_player_games['GAME_ID'].to_list()
            l5_pct = last_5_player_games[col_name].to_list()
            l5_minutes = last_5_player_games['MIN'].to_list()
            l5_games = list(zip(l5_game_ids, l5_pct, l5_minutes))

            l10_game_ids = last_10_player_games['GAME_ID'].to_list()
            l10_pct = last_10_player_games[col_name].to_list()
            l10_minutes = last_10_player_games['MIN'].to_list()
            l10_games = list(zip(l10_game_ids, l10_pct, l10_minutes))

            corrupted_in_last_5 = set()
            corrupted_in_last_10 = set()

            for game_id in l5_game_ids:

                if game_id in config.CORRUPTED_GAME_ROTATION_GAME_IDS:

                    corrupted_in_last_5.add(game_id)

            for game_id in l10_game_ids:

                if game_id in config.CORRUPTED_GAME_ROTATION_GAME_IDS:

                    corrupted_in_last_10.add(game_id)
            
            slice_for_last_5 = len(last_5_player_games) - len(corrupted_in_last_5)
            slice_for_last_10 = len(last_10_player_games) - len(corrupted_in_last_10)

            team_totals_per_player_L5 = team_totals_per_player_df[:slice_for_last_5]
            team_totals_per_player_L10 = team_totals_per_player_df[:slice_for_last_10]

            avg_last_5_team_totals = (
                np.nan
                if len(team_totals_per_player_L5) == 0
                else team_totals_per_player_L5[stat].sum() / len(team_totals_per_player_L5)
            )
            avg_last_10_team_totals = (
                np.nan
                if len(team_totals_per_player_L10) == 0
                else team_totals_per_player_L10[stat].sum() / len(team_totals_per_player_L10)
            )

            avg_last_5_minutes = 0
            avg_last_10_minutes = 0
            avg_last_5_pct_share = 0
            avg_last_10_pct_share = 0

            for gid, curr_pct, minutes in l5_games:

                if gid in corrupted_in_last_5:

                    continue

                avg_last_5_pct_share += curr_pct
                avg_last_5_minutes += minutes
            
            for gid, curr_pct, minutes in l10_games:

                if gid in corrupted_in_last_10:

                    continue

                avg_last_10_pct_share += curr_pct
                avg_last_10_minutes += minutes
        
            avg_last_5_pct_share = (
                np.nan
                if (len(last_5_player_games) - len(corrupted_in_last_5)) == 0
                else avg_last_5_pct_share / (len(last_5_player_games) - len(corrupted_in_last_5))
            )
            avg_last_10_pct_share = (
                np.nan
                if (len(last_10_player_games) - len(corrupted_in_last_10)) == 0
                else avg_last_10_pct_share / (len(last_10_player_games) - len(corrupted_in_last_10))
            )
            avg_last_5_minutes = (
                np.nan
                if (len(last_5_player_games) - len(corrupted_in_last_5)) == 0
                else avg_last_5_minutes / (len(last_5_player_games) - len(corrupted_in_last_5))
            )
            avg_last_10_minutes = (
                np.nan
                if (len(last_10_player_games) - len(corrupted_in_last_10)) == 0
                else avg_last_10_minutes / (len(last_10_player_games) - len(corrupted_in_last_10))
            )
            
            return avg_last_5_pct_share, avg_last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals, avg_last_5_minutes, avg_last_10_minutes

        def find_defensive_rank(conn, positions, team_id, prop):

            rank = 0

            for position in positions:

                df = pd.read_sql_query(f"SELECT * FROM DEFENSE_VS_POSITION_2025_2026 WHERE POSITION = ? ORDER BY {prop} DESC", conn, params=(position,))

                rank += df.index[df['TEAM_ID'] == team_id][0]
            
            return rank / len(positions)
        
        def find_overall_games(player_game_logs_before_curr_date_overall, prop, prop_line):

            if player_game_logs_before_curr_date_overall.empty:

                print(f"Couldn't find player game logs before the overall date for {player_name}")
                return [], np.nan, np.nan, np.nan, np.nan, np.nan

            overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()

            average_L3_minus_line = sum(overall_games_for_current_prop[:3]) / len(overall_games_for_current_prop[:3]) - prop_line
            average_L5_minus_line = sum(overall_games_for_current_prop[:5]) / len(overall_games_for_current_prop[:5]) - prop_line
            average_L7_minus_line = sum(overall_games_for_current_prop[:7]) / len(overall_games_for_current_prop[:7]) - prop_line
            average_L10_minus_line = sum(overall_games_for_current_prop[:10]) / len(overall_games_for_current_prop[:10]) - prop_line

            average_overall_last_20_minus_line = sum(overall_games_for_current_prop[:20]) / len(overall_games_for_current_prop[:20]) - prop_line

            last_5_games_overall = []

            for i in range(min(5, len(overall_games_for_current_prop))):

                last_5_games_overall.append(overall_games_for_current_prop[i])

            return (
                last_5_games_overall, average_overall_last_20_minus_line,
                average_L3_minus_line, average_L5_minus_line,
                average_L7_minus_line, average_L10_minus_line
            )

        def find_opp_games(player_game_logs_before_curr_date_vs_opp, prop, prop_line):

            opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

            if player_game_logs_before_curr_date_vs_opp.empty:

                print(f"Could not find gamelogs for current player before {curr_date}")
                return [], np.nan, np.nan, np.nan, 0

            opp_games_for_current_prop = player_game_logs_before_curr_date_vs_opp[prop].to_list()

            average_L3_minus_line = sum(opp_games_for_current_prop[:3]) / len(opp_games_for_current_prop[:3]) - prop_line
            average_L7_minus_line = sum(opp_games_for_current_prop[:7]) / len(opp_games_for_current_prop[:7]) - prop_line
            average_L10_minus_line = sum(opp_games_for_current_prop[:10]) / len(opp_games_for_current_prop[:10]) - prop_line
            
            last_5_games_vs_opp_list = []

            for i in range(min(5, opp_game_count)):

                last_5_games_vs_opp_list.append(opp_games_for_current_prop[i])
            
            return last_5_games_vs_opp_list, average_L3_minus_line, average_L7_minus_line, average_L10_minus_line, opp_game_count

        today = datetime.strptime(str(curr_date), "%Y-%m-%d").date()
        two_years_from_curr_date = str(today - timedelta(days=730))

        curr_season_player_game_logs = season_game_logs[

            (season_game_logs['PLAYER_ID'] == player_id) &
            (season_game_logs['GAME_DATE'] < str(curr_date)) &
            (season_game_logs['GAME_DATE'] > config.SEASON_START_DATE) &
            (season_game_logs['MIN'] > 0)

        ]

        curr_player_scoreboard = scoreboard[scoreboard['PLAYER_ID'] == player_id]
        player_name = curr_player_scoreboard['PLAYER'].iloc[0]
        opposition_id = curr_player_scoreboard['opposition_team_id'].iloc[0]
        venue = (
            0
            if '@' in scoreboard[scoreboard['PLAYER_ID'] == player_id]['MATCHUP'].iloc[0]
            else 1
        )
        player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

        if len(player_positions) == 0:

            print(f"Could not find player positions for {player_name} ({player_id}) Check scoring_functions.py Line 1511")
            sys.exit(1)

        curr_player_game_logs_vs_opp = game_logs[
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] >= str(two_years_from_curr_date)) &
            (game_logs['OPPOSITION_ID'] == opposition_id) &
            (game_logs['MIN'] > 0)
        ]

        curr_team_total_per_player = team_totals_per_player_df[
            (team_totals_per_player_df['PLAYER_ID'] == player_id) &
            (team_totals_per_player_df['MIN'] > 0)
        ]

        curr_team_total_per_player = curr_team_total_per_player.sort_values('GAME_DATE', ascending=False)
        curr_player_game_logs_vs_opp = curr_player_game_logs_vs_opp.sort_values('GAME_DATE', ascending=False)
        curr_season_player_game_logs = curr_season_player_game_logs.sort_values('GAME_DATE', ascending=False)

        (avg_last_5_pct_share, avg_last_10_pct_share, 
         avg_last_5_team_totals, avg_last_10_team_totals, 
         avg_last_5_minutes, avg_last_10_minutes
        ) = find_team_totals_and_player_share(curr_season_player_game_logs, prop, curr_team_total_per_player)
        
        def_rank = find_defensive_rank(conn, player_positions, current_opposition_id, prop)

        (
            last_5_games_overall, average_overall_last_20_minus_line,
            average_L3_overall_minus_line, average_L5_overall_minus_line,
            average_L7_overall_minus_line, average_L10_overall_minus_line
        ) = find_overall_games(player_game_logs_before_curr_date_overall=curr_season_player_game_logs, prop=prop, prop_line=line)

        (last_5_games_vs_opp_list, 
         average_L3_vs_opp_minus_line, average_L7_vs_opp_minus_line, 
         average_L10_vs_opp_minus_line, opp_game_count) = find_opp_games(player_game_logs_before_curr_date_vs_opp=curr_player_game_logs_vs_opp, prop=prop, prop_line=line)


        if last_5_games_overall == -2:

            return -2

        if np.isnan(minutes_projection) or avg_last_5_minutes == 0 or np.isnan(avg_last_5_pct_share):

            expected_from_last_5_minus_line = np.nan
            expected_from_last_10_minus_line = np.nan
        
        else:

            expected_from_last_5_minus_line = (((avg_last_5_team_totals * avg_last_5_pct_share) / avg_last_5_minutes) * minutes_projection) - line
            expected_from_last_10_minus_line = (((avg_last_10_team_totals * avg_last_10_pct_share) / avg_last_10_minutes) * minutes_projection) - line

        last_game = (
            last_5_games_overall[0] - line
            if len(last_5_games_overall) > 0
            else np.nan
        )
        last_game_vs_opp = (
            last_5_games_vs_opp_list[0] - line
            if len(last_5_games_vs_opp_list) > 0
            else np.nan
        )

        features_dict = {
            
            prop: {
                'LAST_GAME': last_game,
                "AVG_LAST_3_OVERALL": float(average_L3_overall_minus_line),
                "AVG_LAST_5_OVERALL": float(average_L5_overall_minus_line),
                "AVG_LAST_7_OVERALL": float(average_L7_overall_minus_line),
                "AVG_LAST_10_OVERALL": float(average_L10_overall_minus_line),
                'AVERAGE_LAST_20': average_overall_last_20_minus_line,
                'LAST_GAME_VS_OPP': last_game_vs_opp,
                "AVG_LAST_3_VS_OPP": average_L3_vs_opp_minus_line,
                "AVG_LAST_7_VS_OPP": average_L7_vs_opp_minus_line,
                'AVERAGE_LAST_10_VS_OPP': average_L10_vs_opp_minus_line,
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
                f'AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE': float(expected_from_last_5_minus_line),
                f'AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE': float(expected_from_last_10_minus_line),
                'VENUE': venue
            }
        }
    
        return features_dict

    game_logs = game_logs.sort_values("GAME_DATE", ascending=False)

    player_id = game_logs['PLAYER_ID'].iloc[0]
    player_name = game_logs['NAME_CLEAN'].iloc[0]

    today_features = {}

    today_features[player_id] = {'player_name': player_name, 'features': find_features(game_logs, player_id, conn, str(curr_date), scoreboard, config.SEASON_START_DATE, player_positions, team_totals_per_player_df, minutes_projection, season_game_logs)}

    for player_id, values in today_features.items():

        player_name = values['player_name']

        hashmap = values['features']

        if hashmap == -2:

            continue

        for prop, feature in hashmap.items():
            
            df = pd.DataFrame([feature])

            model_file_path = os.path.join(config.XGBOOST_PATH, "scoringv9", f"{prop}_xgboost_model_scoring_v9.pkl")
            model = joblib.load(model_file_path)

            prob_over = model.predict_proba(df)[0][1] * 100

        if prob_over < 50:

            return 0 - (100 - prob_over)
        
        elif prob_over >= 50:

            return prob_over

# similar to v9 but uses minutes projection, position_missing_stat, and player share as well as expected last 5_10
def scoringv10(game_logs, current_opposition_id, prop, line, scoreboard, player_positions, curr_date, team_totals_per_player_df, minutes_projection, season_game_logs, conn):

    def find_features(game_logs, player_id, conn, curr_date, scoreboard, player_positions_df, team_totals_per_player_df, minutes_projection, season_game_logs):

        def find_team_totals_and_player_share(curr_game_logs, stat, team_totals_per_player_df):

            if curr_game_logs.empty:

                return np.nan, np.nan, np.nan, np.nan, np.nan, np.nan

            col_name = f"PCT_{stat}_USAGE"

            curr_game_logs = curr_game_logs.sort_values("GAME_DATE", ascending=False)

            last_5_player_games = curr_game_logs.iloc[:5]
            last_10_player_games = curr_game_logs.iloc[:10]

            l5_game_ids = last_5_player_games['GAME_ID'].to_list()
            l5_pct = last_5_player_games[col_name].to_list()
            l5_minutes = last_5_player_games['MIN'].to_list()
            l5_games = list(zip(l5_game_ids, l5_pct, l5_minutes))

            l10_game_ids = last_10_player_games['GAME_ID'].to_list()
            l10_pct = last_10_player_games[col_name].to_list()
            l10_minutes = last_10_player_games['MIN'].to_list()
            l10_games = list(zip(l10_game_ids, l10_pct, l10_minutes))

            corrupted_in_last_5 = set()
            corrupted_in_last_10 = set()

            for game_id in l5_game_ids:

                if game_id in config.CORRUPTED_GAME_ROTATION_GAME_IDS:

                    corrupted_in_last_5.add(game_id)

            for game_id in l10_game_ids:

                if game_id in config.CORRUPTED_GAME_ROTATION_GAME_IDS:

                    corrupted_in_last_10.add(game_id)
            
            slice_for_last_5 = len(last_5_player_games) - len(corrupted_in_last_5)
            slice_for_last_10 = len(last_10_player_games) - len(corrupted_in_last_10)

            team_totals_per_player_L5 = team_totals_per_player_df[:slice_for_last_5]
            team_totals_per_player_L10 = team_totals_per_player_df[:slice_for_last_10]

            avg_last_5_team_totals = (
                np.nan
                if len(team_totals_per_player_L5) == 0
                else team_totals_per_player_L5[stat].sum() / len(team_totals_per_player_L5)
            )
            avg_last_10_team_totals = (
                np.nan
                if len(team_totals_per_player_L10) == 0
                else team_totals_per_player_L10[stat].sum() / len(team_totals_per_player_L10)
            )

            avg_last_5_minutes = 0
            avg_last_10_minutes = 0
            avg_last_5_pct_share = 0
            avg_last_10_pct_share = 0

            for gid, curr_pct, minutes in l5_games:

                if gid in corrupted_in_last_5:

                    continue

                avg_last_5_pct_share += curr_pct
                avg_last_5_minutes += minutes
            
            for gid, curr_pct, minutes in l10_games:

                if gid in corrupted_in_last_10:

                    continue

                avg_last_10_pct_share += curr_pct
                avg_last_10_minutes += minutes
        
            avg_last_5_pct_share = (
                np.nan
                if (len(last_5_player_games) - len(corrupted_in_last_5)) == 0
                else avg_last_5_pct_share / (len(last_5_player_games) - len(corrupted_in_last_5))
            )
            avg_last_10_pct_share = (
                np.nan
                if (len(last_10_player_games) - len(corrupted_in_last_10)) == 0
                else avg_last_10_pct_share / (len(last_10_player_games) - len(corrupted_in_last_10))
            )
            avg_last_5_minutes = (
                np.nan
                if (len(last_5_player_games) - len(corrupted_in_last_5)) == 0
                else avg_last_5_minutes / (len(last_5_player_games) - len(corrupted_in_last_5))
            )
            avg_last_10_minutes = (
                np.nan
                if (len(last_10_player_games) - len(corrupted_in_last_10)) == 0
                else avg_last_10_minutes / (len(last_10_player_games) - len(corrupted_in_last_10))
            )
            
            return avg_last_5_pct_share, avg_last_10_pct_share, avg_last_5_team_totals, avg_last_10_team_totals, avg_last_5_minutes, avg_last_10_minutes

        def find_defensive_rank(conn, positions, team_id, prop):

            rank = 0

            for position in positions:

                df = pd.read_sql_query(f"SELECT * FROM DEFENSE_VS_POSITION_2025_2026 WHERE POSITION = ? ORDER BY {prop} DESC", conn, params=(position,))

                rank += df.index[df['TEAM_ID'] == team_id][0]
            
            return rank / len(positions)
        
        def find_overall_games(player_game_logs_before_curr_date_overall, prop, prop_line):

            if player_game_logs_before_curr_date_overall.empty:

                print(f"Couldn't find player game logs before the overall date for {player_name}")
                return [], np.nan, np.nan, np.nan, np.nan, np.nan

            overall_games_for_current_prop = player_game_logs_before_curr_date_overall[prop].to_list()

            average_L3_minus_line = sum(overall_games_for_current_prop[:3]) / len(overall_games_for_current_prop[:3]) - prop_line
            average_L5_minus_line = sum(overall_games_for_current_prop[:5]) / len(overall_games_for_current_prop[:5]) - prop_line
            average_L7_minus_line = sum(overall_games_for_current_prop[:7]) / len(overall_games_for_current_prop[:7]) - prop_line
            average_L10_minus_line = sum(overall_games_for_current_prop[:10]) / len(overall_games_for_current_prop[:10]) - prop_line

            average_overall_last_20_minus_line = sum(overall_games_for_current_prop[:20]) / len(overall_games_for_current_prop[:20]) - prop_line

            last_5_games_overall = []

            for i in range(min(5, len(overall_games_for_current_prop))):

                last_5_games_overall.append(overall_games_for_current_prop[i])

            return (
                last_5_games_overall, average_overall_last_20_minus_line,
                average_L3_minus_line, average_L5_minus_line,
                average_L7_minus_line, average_L10_minus_line
            )

        def find_opp_games(player_game_logs_before_curr_date_vs_opp, prop, prop_line):

            opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

            if player_game_logs_before_curr_date_vs_opp.empty:

                print(f"Could not find gamelogs for current player before {curr_date}")
                return [], np.nan, np.nan, np.nan, 0

            opp_games_for_current_prop = player_game_logs_before_curr_date_vs_opp[prop].to_list()

            average_L3_minus_line = sum(opp_games_for_current_prop[:3]) / len(opp_games_for_current_prop[:3]) - prop_line
            average_L7_minus_line = sum(opp_games_for_current_prop[:7]) / len(opp_games_for_current_prop[:7]) - prop_line
            average_L10_minus_line = sum(opp_games_for_current_prop[:10]) / len(opp_games_for_current_prop[:10]) - prop_line
            
            last_5_games_vs_opp_list = []

            for i in range(min(5, opp_game_count)):

                last_5_games_vs_opp_list.append(opp_games_for_current_prop[i])
            
            return last_5_games_vs_opp_list, average_L3_minus_line, average_L7_minus_line, average_L10_minus_line, opp_game_count

        def find_position_missing_stats(conn, curr_date, positions, team_id, stat):

            dfs = []

            for position in positions:

                df = pd.read_sql_query("""

                        SELECT d.*
                        FROM DNPS d
                        JOIN PLAYER_POSITIONS p
                            ON p.PLAYER_ID = d.PLAYER_ID
                        WHERE p.POSITION = ?
                        AND d.GAME_DATE = ?
                        AND d.TEAM_ID = ?
                                    
                    """, conn, params=(position, str(curr_date), team_id))

                if not df.empty:

                    dfs.append(df)
            
            if not dfs:

                return 0

            cat = pd.concat(dfs, ignore_index=True)

            total_pos_stat = cat.drop_duplicates('PLAYER_ID')[f'AVERAGE_{stat}'].sum()
            
            return total_pos_stat

        today = datetime.strptime(str(curr_date), "%Y-%m-%d").date()
        two_years_from_curr_date = str(today - timedelta(days=730))

        curr_season_player_game_logs = season_game_logs[

            (season_game_logs['PLAYER_ID'] == player_id) &
            (season_game_logs['GAME_DATE'] < str(curr_date)) &
            (season_game_logs['GAME_DATE'] > config.SEASON_START_DATE) &
            (season_game_logs['MIN'] > 0)

        ]

        curr_player_scoreboard = scoreboard[scoreboard['PLAYER_ID'] == player_id]
        team_id = curr_player_scoreboard['TeamID'].iloc[0]
        player_name = curr_player_scoreboard['PLAYER'].iloc[0]
        opposition_id = curr_player_scoreboard['opposition_team_id'].iloc[0]
        venue = (
            0
            if '@' in scoreboard[scoreboard['PLAYER_ID'] == player_id]['MATCHUP'].iloc[0]
            else 1
        )
        player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

        position_missing_stat = find_position_missing_stats(conn, curr_date, player_positions, prop)

        if len(player_positions) == 0:

            print(f"Could not find player positions for {player_name} ({player_id}) Check scoring_functions.py Line 1511")
            sys.exit(1)

        curr_player_game_logs_vs_opp = game_logs[
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] >= str(two_years_from_curr_date)) &
            (game_logs['OPPOSITION_ID'] == opposition_id) &
            (game_logs['MIN'] > 0)
        ]

        curr_team_total_per_player = team_totals_per_player_df[
            (team_totals_per_player_df['PLAYER_ID'] == player_id) &
            (team_totals_per_player_df['MIN'] > 0)
        ]

        curr_team_total_per_player = curr_team_total_per_player.sort_values('GAME_DATE', ascending=False)
        curr_player_game_logs_vs_opp = curr_player_game_logs_vs_opp.sort_values('GAME_DATE', ascending=False)
        curr_season_player_game_logs = curr_season_player_game_logs.sort_values('GAME_DATE', ascending=False)

        (avg_last_5_pct_share, avg_last_10_pct_share, 
         avg_last_5_team_totals, avg_last_10_team_totals, 
         avg_last_5_minutes, avg_last_10_minutes
        ) = find_team_totals_and_player_share(curr_season_player_game_logs, prop, curr_team_total_per_player)
        
        def_rank = find_defensive_rank(conn, player_positions, current_opposition_id, prop)

        (
            last_5_games_overall, average_overall_last_20_minus_line,
            average_L3_overall_minus_line, average_L5_overall_minus_line,
            average_L7_overall_minus_line, average_L10_overall_minus_line
        ) = find_overall_games(player_game_logs_before_curr_date_overall=curr_season_player_game_logs, prop=prop, prop_line=line)

        (last_5_games_vs_opp_list, 
         average_L3_vs_opp_minus_line, average_L7_vs_opp_minus_line, 
         average_L10_vs_opp_minus_line, opp_game_count) = find_opp_games(player_game_logs_before_curr_date_vs_opp=curr_player_game_logs_vs_opp, prop=prop, prop_line=line)


        if last_5_games_overall == -2:

            return -2

        if np.isnan(minutes_projection) or avg_last_5_minutes == 0 or np.isnan(avg_last_5_pct_share):

            expected_from_last_5_minus_line = np.nan
            expected_from_last_10_minus_line = np.nan
        
        else:

            expected_from_last_5_minus_line = (((avg_last_5_team_totals * avg_last_5_pct_share) / avg_last_5_minutes) * minutes_projection) - line
            expected_from_last_10_minus_line = (((avg_last_10_team_totals * avg_last_10_pct_share) / avg_last_10_minutes) * minutes_projection) - line

        last_game = (
            last_5_games_overall[0] - line
            if len(last_5_games_overall) > 0
            else np.nan
        )
        last_game_vs_opp = (
            last_5_games_vs_opp_list[0] - line
            if len(last_5_games_vs_opp_list) > 0
            else np.nan
        )

        features_dict = {
            
            prop: {
                'LAST_GAME': last_game,
                "AVG_LAST_3_OVERALL": float(average_L3_overall_minus_line),
                "AVG_LAST_5_OVERALL": float(average_L5_overall_minus_line),
                "AVG_LAST_7_OVERALL": float(average_L7_overall_minus_line),
                "AVG_LAST_10_OVERALL": float(average_L10_overall_minus_line),
                'AVERAGE_LAST_20': average_overall_last_20_minus_line,
                'LAST_GAME_VS_OPP': last_game_vs_opp,
                "AVG_LAST_3_VS_OPP": average_L3_vs_opp_minus_line,
                "AVG_LAST_7_VS_OPP": average_L7_vs_opp_minus_line,
                'AVERAGE_LAST_10_VS_OPP': average_L10_vs_opp_minus_line,
                'DEF_RANK': float(def_rank),
                'OPP_GAME_COUNT': opp_game_count,
                'MINUTES_PROJECTION': minutes_projection,
                'POSITION_MISSING_STAT': position_missing_stat,
                f'AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE': float(expected_from_last_5_minus_line),
                f'AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE': float(expected_from_last_10_minus_line),
                'VENUE': venue
            }
        }
    
        return features_dict

    game_logs = game_logs.sort_values("GAME_DATE", ascending=False)

    player_id = game_logs['PLAYER_ID'].iloc[0]
    player_name = game_logs['NAME_CLEAN'].iloc[0]

    today_features = {}

    today_features[player_id] = {'player_name': player_name, 'features': find_features(game_logs, player_id, conn, str(curr_date), scoreboard, player_positions, team_totals_per_player_df, minutes_projection, season_game_logs)}

    for player_id, values in today_features.items():

        player_name = values['player_name']

        hashmap = values['features']

        if hashmap == -2:

            continue

        for prop, feature in hashmap.items():
            
            df = pd.DataFrame([feature])

            model_file_path = os.path.join(config.XGBOOST_PATH, "scoringv9", f"{prop}_xgboost_model_scoring_v9.pkl")
            model = joblib.load(model_file_path)

            prob_over = model.predict_proba(df)[0][1] * 100

        if prob_over < 50:

            return 0 - (100 - prob_over)
        
        elif prob_over >= 50:

            return prob_over

if __name__ == '__main__':

    config = load_config()
    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ? ORDER BY GAME_DATE DESC", conn, params=(config.SEASON_START_DATE,))
    team_totals_df = pd.read_sql_query("SELECT * FROM TEAM_STATS_2025_2026", conn)
