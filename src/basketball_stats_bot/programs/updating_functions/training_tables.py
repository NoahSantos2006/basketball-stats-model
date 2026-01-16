import pandas as pd
from datetime import datetime, timedelta, date
import numpy as np
from zoneinfo import ZoneInfo
import statistics
import os
import joblib
import sys
import sqlite3

from basketball_stats_bot.config import load_config


def update_props_training_table(season_start_date, conn):

    config = load_config()

    def find_minutes_projection(season_game_logs, curr_scoreboard, positions_df, season_start_date, curr_date, player_id, conn):

        def find_minute_projection_features(conn, season_start_date, curr_date, season_game_logs, curr_scoreboard, positions_df, player_id):

            def avg_last_3_5_7_10(game_logs, player_id, curr_date):

                game_logs = game_logs[
                    (game_logs['GAME_DATE'] < str(curr_date)) &
                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['MIN'] > 0)
                ]

                game_logs = game_logs.sort_values("GAME_DATE", ascending=False)

                minutes_list = game_logs['MIN'].to_list()
                last_3 = []
                last_5 = []
                last_7 = []
                last_10 = []

                if len(minutes_list) == 0:

                    return np.nan, np.nan, np.nan, np.nan

                i = 0

                while i < min(10, len(minutes_list)):

                    if len(last_3) < 3:
                        last_3.append(minutes_list[i])       
                    if len(last_5) < 5:
                        last_5.append(minutes_list[i])
                    if len(last_7) < 7:
                        last_7.append(minutes_list[i])
                    if len(last_10) < 10:
                        last_10.append(minutes_list[i])

                    i += 1
                
                curr_average = sum(minutes_list) / len(minutes_list)

                while len(last_10) < 10:

                    if len(last_3) < 3:
                        last_3.append(curr_average)       
                    if len(last_5) < 5:
                        last_5.append(curr_average)
                    if len(last_7) < 7:
                        last_7.append(curr_average)
                    if len(last_10) < 10:
                        last_10.append(curr_average)

                average_last_3 = float(sum(last_3) / 3)
                average_last_5 = float(sum(last_5) / 5)
                average_last_7 = float(sum(last_7) / 7)
                average_last_10 = float(sum(last_10) / 10)

                return average_last_3, average_last_5, average_last_7, average_last_10

            def minutes_trend_5(curr_date, player_id, player_name, season_game_logs):
                
                curr_player_game_logs = season_game_logs[
                    (season_game_logs['GAME_DATE'] < str(curr_date)) &
                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['MIN'] > 0)
                ]
                
                minutes_list = curr_player_game_logs['MIN'].to_list()

                last_5 = []
                average = 0

                if len(minutes_list) == 0:

                    print(f"Could not find games with minutes before {curr_date} for {player_name}.")
                    return np.nan
                
                last_5 = minutes_list[:5]

                if len(last_5) < 2:

                    return np.nan
                
                average = sum(last_5) / len(last_5)

                while len(last_5) < 5:

                    last_5.append(average)

                slope = (last_5[-1] - last_5[0]) / 4

                return slope

            def find_position_missing_minutes(conn, curr_date, positions, team_id):

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

                total_pos_minutes = cat.drop_duplicates('PLAYER_ID')['AVERAGE_MINUTES'].sum()
                
                return min(48, total_pos_minutes)

            def find_last_10_std_dev(curr_date, player_id, player_name, game_logs):

                current_player_game_logs = game_logs[
                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['GAME_DATE'] < curr_date) &
                    (game_logs['MIN'] > 0)
                ]

                if current_player_game_logs.empty:

                    print(f"Could not find game logs before {curr_date} for {player_name}")
                    return np.nan

                last_10_games = current_player_game_logs.iloc[:10]['MIN'].to_list()

                if len(last_10_games) < 5:

                    return np.nan

                std_dev = statistics.stdev(last_10_games)

                return std_dev

            def find_days_rest(curr_date, player_id, game_logs, season_start_date_str):

                curr_player_game_logs = game_logs[

                    (game_logs['PLAYER_ID'] == player_id) &
                    (game_logs['GAME_DATE'] < curr_date) &
                    (game_logs['MIN'] > 0)
                ]

                last_game_date = datetime.strptime(curr_date, "%Y-%m-%d").date() - timedelta(days=1)
                season_start_date = datetime.strptime(season_start_date_str, "%Y-%m-%d").date()

                days_rest = 0

                while last_game_date >= season_start_date:

                    if not curr_player_game_logs[curr_player_game_logs['GAME_DATE'] == str(last_game_date)].empty:

                        return days_rest

                    days_rest += 1
                    last_game_date -= timedelta(days=1)
                
                return np.nan

            def find_total_games_played_this_season(curr_date, player_id, game_logs):

                current_player_season_game_logs = len(
                    game_logs[

                        (game_logs['PLAYER_ID'] == player_id) &
                        (game_logs['GAME_DATE'] < curr_date) &
                        (game_logs['MIN'] > 0)
                    ]
                )

                return current_player_season_game_logs

            def find_if_back_to_back(curr_date, player_id, game_logs):

                day_before_curr_date = datetime.strptime(curr_date, "%Y-%m-%d").date() - timedelta(days=1)

                if game_logs[

                    (game_logs['GAME_DATE'] == str(day_before_curr_date)) &
                    (game_logs['PLAYER_ID'] == player_id)

                ].empty:
                    
                    return 0

                return 1

            def find_games_started_last_5(curr_date, player_id, season_game_logs):

                player_game_logs = season_game_logs[
                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['GAME_DATE'] < curr_date) &
                    (season_game_logs['MIN'] > 0)
                ].iloc[:5]

                if player_game_logs.empty:

                    return np.nan

                return len(player_game_logs[
                    (player_game_logs['STARTER'] == 1)
                ])

            def find_games_played_last_5_10_compared_to_team(curr_date, player_id, season_game_logs):

                player_logs = season_game_logs[

                    (season_game_logs['PLAYER_ID'] == player_id) &
                    (season_game_logs['GAME_DATE'] < str(curr_date)) 
                    
                ].iloc[:10]

                if player_logs.empty:

                    return np.nan, np.nan

                team_id = player_logs['TEAM_ID'].iloc[0]

                team_logs = season_game_logs[

                    (season_game_logs['TEAM_ID'] == team_id) &
                    (season_game_logs['GAME_DATE'] < str(curr_date))

                ].drop_duplicates("GAME_DATE").iloc[:10]

                games_played_last_5 = 0
                games_played_last_10 = 0
                team_logs_game_id_list = team_logs['GAME_ID'].to_list()
                player_logs_game_id_list = player_logs['GAME_ID'].to_list()

                i = 0

                for gameId in team_logs_game_id_list:
                    
                    if gameId in player_logs_game_id_list:

                        if games_played_last_5 < 5:

                            games_played_last_5 += 1
                        
                        games_played_last_10 += 1
                        
                    i += 1

                    if i == 10:

                        break
                
                return games_played_last_5, games_played_last_10

            positions = positions_df[positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

            team_id = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['TeamID'].iloc[0]
            player_name = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]

            print(f"Finding minutes projection features for {player_name}...")

            average_last_3, average_last_5, average_last_7, average_last_10 = avg_last_3_5_7_10(season_game_logs, player_id, str(curr_date))
            minute_trend = minutes_trend_5(str(curr_date), player_id, player_name, season_game_logs)
            position_missing_minutes = find_position_missing_minutes(conn, str(curr_date), positions, int(team_id))
            last_10_std_dev = find_last_10_std_dev(str(curr_date), player_id, player_name, season_game_logs)
            days_rest = find_days_rest(str(curr_date), player_id, season_game_logs, season_start_date)
            total_games_played_this_season = find_total_games_played_this_season(str(curr_date), player_id, season_game_logs)
            is_back_to_back = find_if_back_to_back(str(curr_date), player_id, season_game_logs)
            games_started_last_5 = find_games_started_last_5(str(curr_date), player_id, season_game_logs)
            games_played_last_5, games_played_last_10 = find_games_played_last_5_10_compared_to_team(str(curr_date), player_id, season_game_logs)

            features_dict = {
                "AVERAGE_LAST_3": average_last_3, 
                "AVERAGE_LAST_5": average_last_5, 
                "AVERAGE_LAST_7": average_last_7, 
                "AVERAGE_LAST_10": average_last_10, 
                "MINUTE_TREND": minute_trend, 
                "POSITION_MISSING_MINUTES": position_missing_minutes, 
                "LAST_10_STANDARD_DEVIATION": last_10_std_dev,
                "DAYS_OF_REST": days_rest,
                "TOTAL_GAMES_PLAYED_THIS_SEASON": total_games_played_this_season,
                "IS_BACK_TO_BACK": is_back_to_back,
                "GAMES_STARTED_LAST_5": games_started_last_5,
                "GAMES_PLAYED_LAST_5": games_played_last_5,
                "GAMES_PLAYED_LAST_10": games_played_last_10,
            }


            return features_dict

        minutes_projection_features = pd.DataFrame([find_minute_projection_features(
            conn=conn, 
            season_start_date=season_start_date, 
            curr_date=curr_date,
            season_game_logs=season_game_logs,
            curr_scoreboard=curr_scoreboard,
            positions_df=positions_df,
            player_id = player_id
        )])

        minutes_projection_model_path = os.path.join(config.XGBOOST_PATH, "minutes_projection_model.pkl")

        model = joblib.load(minutes_projection_model_path)

        minutes_projection = model.predict(minutes_projection_features)[0]

        return float(minutes_projection)

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

    cursor = conn.cursor()

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

    nofind_player_ids = []

    #just curr_date in string form instead of date form
    today = datetime.now(ZoneInfo(config.TIMEZONE)).date()
    check_for_latest_date = pd.read_sql_query("SELECT * FROM PROPS_TRAINING_TABLE ORDER BY GAME_DATE DESC", conn)
    latest_date_str = check_for_latest_date['GAME_DATE'].iloc[0]
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date() + timedelta(days=1)

    # all games during and before the curr_date ordered by game date descending
    game_logs = pd.read_sql_query('SELECT * FROM player_game_logs ORDER BY GAME_DATE DESC', conn)
    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ? ORDER BY GAME_DATE DESC", conn, params=(season_start_date,))
    system = pd.read_sql_query('SELECT * FROM SYSTEM', conn)
    scoreboard = pd.read_sql_query('SELECT * FROM SCOREBOARD_TO_ROSTER', conn)
    player_positions_df = pd.read_sql_query('SELECT * FROM PLAYER_POSITIONS', conn,)
    team_totals_per_player_df = pd.read_sql_query("SELECT * FROM TEAM_TOTALS_PER_PLAYER", conn)

    while curr_date < today:

        game_date = str(curr_date)
        two_years_from_curr_date = str(curr_date - timedelta(days=730))

        curr_game_logs = game_logs[game_logs['GAME_DATE'] <= str(curr_date)].copy()
        curr_game_logs = curr_game_logs.sort_values("GAME_DATE", ascending=False)

        if curr_game_logs[curr_game_logs['GAME_DATE'] == game_date].empty and game_date < str(today):

            print(f"Game logs for {curr_date} not found. Check training_tables.py Line 536.")
            curr_date += timedelta(days=1)
            continue

        curr_scoreboard = scoreboard[scoreboard['date'] == str(curr_date)]
        curr_system = system[system['DATE'] == str(curr_date)]

        if curr_system.empty:

            print(f"Could not find game props for {curr_date}..")
            curr_date += timedelta(days=1)
            return

        # finds all player ids for the players that are playing during curr_date
        player_ids = curr_scoreboard.drop_duplicates('PLAYER_ID')['PLAYER_ID'].to_list()
        
        # iterates through all players playing during curr_date
        for player_id in player_ids:

            minutes_projection = find_minutes_projection(
                season_game_logs=season_game_logs, 
                curr_scoreboard=curr_scoreboard, 
                positions_df=player_positions_df, 
                season_start_date=season_start_date, 
                curr_date=curr_date, 
                player_id=player_id,
                conn=conn
            )

            player_name = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]
            team_id = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['TeamID'].iloc[0]
            matchup = (
                0
                if '@' in curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['MATCHUP'].iloc[0]
                else 1
            )
            
            curr_player_game_logs = curr_game_logs[curr_game_logs['PLAYER_ID'] == player_id]

            if curr_player_game_logs[curr_player_game_logs['GAME_DATE'] == game_date].empty:
                
                print(f"Could not find a game log for {player_name} on {game_date}.")
                nofind_player_ids.append(player_id)
                continue
                
            

            # finds player positions
            player_positions = player_positions_df[player_positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()

            # finds the game_id for the player's game today
            game_id = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] == game_date)
            ]['GAME_ID'].iloc[0]

            player_box_score = curr_player_game_logs[curr_player_game_logs['GAME_DATE'] == game_date]

            if player_box_score['MIN'].iloc[0] == 0:

                continue

            # opposition_id for the current matchup of curr_date
            opposition_id = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['opposition_team_id'].iloc[0]

            # finds player games against opp before current date
            player_game_logs_before_curr_date_vs_opp = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] < game_date) & # before curr date
                (curr_player_game_logs['OPPOSITION_ID'] == opposition_id) & # if the opposition id is the same as the scoreboard's
                (curr_player_game_logs['GAME_DATE'] >= two_years_from_curr_date) & # needs to be later than two years ago
                (curr_player_game_logs['MIN'] > 0) # needs to play more than 0 minutes
            ]

            # finds total number of games against opp
            opp_game_count = len(player_game_logs_before_curr_date_vs_opp)

            # finds player games overall during the season
            player_game_logs_before_curr_date_overall = curr_player_game_logs[
                (curr_player_game_logs['GAME_DATE'] < game_date) & # before curr date
                (curr_player_game_logs['GAME_DATE'] >= season_start_date) & # needs to be during this season
                (curr_player_game_logs['MIN'] > 0) # needs to player more than 0 minutes
            ]

            games_played_this_season = len(player_game_logs_before_curr_date_overall)

            extra_pct_dict = {}
            share_dict = {}

            curr_team_total_per_player = team_totals_per_player_df[
                (team_totals_per_player_df['PLAYER_ID'] == player_id) &
                (team_totals_per_player_df['MIN'] > 0)
            ]

            player_game_logs_before_curr_date_overall = player_game_logs_before_curr_date_overall.sort_values("GAME_DATE", ascending=False)
            player_game_logs_before_curr_date_vs_opp = player_game_logs_before_curr_date_vs_opp.sort_values("GAME_DATE", ascending=False)
            curr_team_total_per_player = curr_team_total_per_player.sort_values('GAME_DATE', ascending=False)

            for prop in props:

                current_player_prop_row = curr_system[
                    (curr_system['PLAYER_ID'] == player_id) &
                    (curr_system['PROP'] == translation[prop])
                ]

                if current_player_prop_row.empty:

                    continue
                
                (avg_last_5_pct_share, avg_last_10_pct_share, 
                 avg_last_5_team_totals, avg_last_10_team_totals, 
                 avg_last_5_minutes, avg_last_10_minutes) = find_team_totals_and_player_share(
                    curr_game_logs=player_game_logs_before_curr_date_overall, 
                    stat=prop,
                    team_totals_per_player_df=curr_team_total_per_player,
                )

                prop_line = current_player_prop_row['LINE'].iloc[0]

                if np.isnan(minutes_projection) or avg_last_5_minutes == 0 or np.isnan(avg_last_5_pct_share):

                    print(minutes_projection, avg_last_5_minutes, avg_last_5_pct_share)

                    expected_from_last_5_minus_line = np.nan
                    expected_from_last_10_minus_line = np.nan
                
                else:

                    expected_from_last_5_minus_line = (((avg_last_5_team_totals * avg_last_5_pct_share) / avg_last_5_minutes) * minutes_projection) - prop_line
                    expected_from_last_10_minus_line = (((avg_last_10_team_totals * avg_last_10_pct_share) / avg_last_10_minutes) * minutes_projection) - prop_line

                extra_pct_dict[f"AVERAGE_LAST_5_EXPECTED_{prop}_MINUS_LINE"] = expected_from_last_5_minus_line
                extra_pct_dict[f"AVERAGE_LAST_10_EXPECTED_{prop}_MINUS_LINE"] = expected_from_last_10_minus_line

                share_dict[f"AVG_LAST_5_{prop}_SHARE"] = avg_last_5_pct_share
                share_dict[f"AVG_LAST_10_{prop}_SHARE"] = avg_last_10_pct_share

                stat_line_for_current_game = player_box_score[prop].iloc[0]

                if prop not in {'BLK', 'STL'}:
                
                    pos_mis_stat = find_position_missing_stats(conn, str(curr_date), player_positions, int(team_id), prop)

                    cursor.execute("""

                    UPDATE PROPS_TRAINING_TABLE
                    SET 
                        POSITION_MISSING_STAT = ?
                    WHERE PROP = ?
                    AND GAME_DATE = ?
                    AND PLAYER_ID = ?
                                
                    """, (pos_mis_stat, prop, str(curr_date), player_id))

                if stat_line_for_current_game > prop_line:

                    result = 1
                
                else:

                    result = 0

                print(f"Updating the PROPS_TRAINING_TABLE for {player_name} on {game_date} for prop {prop}...")
                # defensive rank based on team and statline

                def_rank = find_defensive_rank(conn, player_positions, opposition_id, prop)
                (
                last_5_games_overall, average_overall_last_20_minus_line,
                average_L3_overall_minus_line, average_L5_overall_minus_line,
                average_L7_overall_minus_line, average_L10_overall_minus_line
                ) = find_overall_games(player_game_logs_before_curr_date_overall, prop, prop_line)
            
                (
                    last_5_games_vs_opp, average_L3_vs_opp_minus_line, 
                    average_L7_vs_opp_minus_line, average_L10_vs_opp_minus_line, 
                    opp_game_count
                ) = find_opp_games(player_game_logs_before_curr_date_vs_opp, prop, prop_line)
                
                last_game = (
                    last_5_games_overall[0] - prop_line
                    if len(last_5_games_overall) > 0
                    else np.nan
                )
                second_last_game = (
                    last_5_games_overall[1] - prop_line
                    if len(last_5_games_overall) > 1
                    else np.nan
                )
                third_last_game = (
                    last_5_games_overall[2] - prop_line
                    if len(last_5_games_overall) > 2
                    else np.nan
                )
                fourth_last_game = (
                    last_5_games_overall[3] - prop_line
                    if len(last_5_games_overall) > 3
                    else np.nan
                )
                fifth_last_game = (
                    last_5_games_overall[4] - prop_line
                    if len(last_5_games_overall) > 4
                    else np.nan
                )
                last_game_vs_opp = (
                    last_5_games_vs_opp[0] - prop_line
                    if len(last_5_games_vs_opp) > 0
                    else np.nan
                )
                second_last_game_vs_opp = (
                    last_5_games_vs_opp[1] - prop_line
                    if len(last_5_games_vs_opp) > 1
                    else np.nan
                )
                third_last_game_vs_opp = (
                    last_5_games_vs_opp[2] - prop_line
                    if len(last_5_games_vs_opp) > 2
                    else np.nan
                )
                fourth_last_game_vs_opp = (
                    last_5_games_vs_opp[3] - prop_line
                    if len(last_5_games_vs_opp) > 3
                    else np.nan
                )
                fifth_last_game_vs_opp = (
                    last_5_games_vs_opp[4] - prop_line
                    if len(last_5_games_vs_opp) > 4
                    else np.nan
                )

                training_table_row = {
                    'GAME_DATE': game_date,
                    'GAME_ID': game_id,
                    'PLAYER_NAME': player_name,
                    'PLAYER_ID': player_id,
                    'PROP': prop,
                    'PROP_LINE': prop_line,
                    'LAST_GAME': last_game,
                    'SECOND_LAST_GAME': second_last_game,
                    'THIRD_LAST_GAME': third_last_game,
                    'FOURTH_LAST_GAME': fourth_last_game,
                    'FIFTH_LAST_GAME': fifth_last_game,
                    'AVG_LAST_3_OVERALL': average_L3_overall_minus_line,
                    'AVG_LAST_5_OVERALL': average_L5_overall_minus_line,
                    'AVG_LAST_7_OVERALL': average_L7_overall_minus_line,
                    'AVG_LAST_10_OVERALL': average_L10_overall_minus_line,
                    'AVERAGE_LAST_20': average_overall_last_20_minus_line,
                    'LAST_GAME_VS_OPP': last_game_vs_opp,
                    'SECOND_LAST_GAME_VS_OPP': second_last_game_vs_opp,
                    'THIRD_LAST_GAME_VS_OPP': third_last_game_vs_opp,
                    'FOURTH_LAST_GAME_VS_OPP': fourth_last_game_vs_opp,
                    'FIFTH_LAST_GAME_VS_OPP': fifth_last_game_vs_opp,
                    'AVG_LAST_3_VS_OPP': average_L3_vs_opp_minus_line,
                    'AVG_LAST_7_VS_OPP': average_L7_vs_opp_minus_line,
                    'AVERAGE_LAST_10_VS_OPP': average_L10_vs_opp_minus_line,
                    'DEF_RANK': float(def_rank),
                    'OPP_GAME_COUNT': opp_game_count,
                    'TARGET': int(result),
                    'VENUE': matchup,
                    'GAMES_PLAYED_THIS_SEASON': games_played_this_season,
                    'MINUTES_PROJECTION': float(minutes_projection),
                    "POSITION_MISSING_STAT": pos_mis_stat

                }

                placeholders = ", ".join(['?']*len(training_table_row))
                columns = ", ".join(training_table_row.keys())
                stats = list(training_table_row.values())

                query = f"INSERT OR REPLACE INTO PROPS_TRAINING_TABLE ({columns}) VALUES ({placeholders})"

                cursor.execute(query, stats)
            
            for k, v in extra_pct_dict.items():

                cursor.execute(f"UPDATE PROPS_TRAINING_TABLE SET {k} = ? WHERE GAME_DATE = ? AND PLAYER_ID = ?", (v, str(curr_date), player_id))
            
            for k, v in share_dict.items():

                cursor.execute(f"UPDATE PROPS_TRAINING_TABLE SET {k} = ? WHERE GAME_DATE = ? AND PLAYER_ID = ?", (v, str(curr_date), player_id))
        
        conn.commit()
    
        curr_date += timedelta(days=1)

def update_minutes_projection_features_table(conn, season_start_date):

    def avg_last_3_5_7_10(game_logs, player_id, curr_date):

        game_logs = game_logs[
            (game_logs['GAME_DATE'] < str(curr_date)) &
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['MIN'] > 0)
        ].sort_values("GAME_DATE", ascending=False)

        minutes_list = game_logs['MIN'].to_list()
        last_3 = []
        last_5 = []
        last_7 = []
        last_10 = []

        if len(minutes_list) == 0:

            return np.nan, np.nan, np.nan, np.nan

        i = 0

        while i < min(10, len(minutes_list)):

            if len(last_3) < 3:
                last_3.append(minutes_list[i])       
            if len(last_5) < 5:
                last_5.append(minutes_list[i])
            if len(last_7) < 7:
                last_7.append(minutes_list[i])
            if len(last_10) < 10:
                last_10.append(minutes_list[i])

            i += 1
        
        curr_average = sum(minutes_list) / len(minutes_list)

        while len(last_10) < 10:

            if len(last_3) < 3:
                last_3.append(curr_average)       
            if len(last_5) < 5:
                last_5.append(curr_average)
            if len(last_7) < 7:
                last_7.append(curr_average)
            if len(last_10) < 10:
                last_10.append(curr_average)

        average_last_3 = float(sum(last_3) / 3)
        average_last_5 = float(sum(last_5) / 5)
        average_last_7 = float(sum(last_7) / 7)
        average_last_10 = float(sum(last_10) / 10)

        return average_last_3, average_last_5, average_last_7, average_last_10

    def minutes_trend_5(curr_date, player_id, player_name, season_game_logs):
        
        curr_player_game_logs = season_game_logs[
            (season_game_logs['GAME_DATE'] < str(curr_date)) &
            (season_game_logs['PLAYER_ID'] == player_id) &
            (season_game_logs['MIN'] > 0)
        ].sort_values("GAME_DATE", ascending=False)
        
        minutes_list = curr_player_game_logs['MIN'].to_list()

        last_5 = []

        if len(minutes_list) == 0:

            print(f"Could not find games with minutes before {curr_date} for {player_name}.")
            return np.nan
        
        last_5 = minutes_list[:5]

        if len(last_5) < 2:

            return np.nan
        
        [25, 30, 23, 30, 23]

        slope = (last_5[0] - last_5[-1]) / (len(last_5) - 1)

        return slope

    def find_position_missing_minutes(conn, curr_date, positions, team_id):

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

        total_pos_minutes = cat.drop_duplicates('PLAYER_ID')['AVERAGE_MINUTES'].sum()
        
        return total_pos_minutes

    def find_last_10_std_dev(curr_date, player_id, player_name, game_logs):

        current_player_game_logs = game_logs[
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] < curr_date) &
            (game_logs['MIN'] > 0)
        ].sort_values("GAME_DATE", ascending=False)

        if current_player_game_logs.empty:

            print(f"Could not find game logs before {curr_date} for {player_name}")
            return np.nan
        
        current_player_game_logs = current_player_game_logs.sort_values("GAME_DATE", ascending=False)

        last_10_games = current_player_game_logs.iloc[:10]['MIN'].to_list()

        if len(last_10_games) < 5:

            return np.nan

        std_dev = statistics.stdev(last_10_games)

        return std_dev

    def find_days_rest(curr_date, player_id, game_logs, season_start_date_str):

        curr_player_game_logs = game_logs[

            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['GAME_DATE'] < curr_date) &
            (game_logs['MIN'] > 0)
        ]

        last_game_date = datetime.strptime(curr_date, "%Y-%m-%d").date() - timedelta(days=1)
        season_start_date = datetime.strptime(season_start_date_str, "%Y-%m-%d").date()

        days_rest = 0

        while last_game_date >= season_start_date:

            if not curr_player_game_logs[curr_player_game_logs['GAME_DATE'] == str(last_game_date)].empty:

                return days_rest

            days_rest += 1
            last_game_date -= timedelta(days=1)
        
        return np.nan

    def find_total_games_played_this_season(curr_date, player_id, game_logs):

        current_player_season_game_logs = len(
            game_logs[

                (game_logs['PLAYER_ID'] == player_id) &
                (game_logs['GAME_DATE'] < curr_date) &
                (game_logs['MIN'] > 0)
            ]
        )

        return current_player_season_game_logs

    def find_if_back_to_back(curr_date, player_id, game_logs):

        day_before_curr_date = datetime.strptime(curr_date, "%Y-%m-%d").date() - timedelta(days=1)

        if game_logs[

            (game_logs['GAME_DATE'] == str(day_before_curr_date)) &
            (game_logs['PLAYER_ID'] == player_id) &
            (game_logs['MIN'] > 0)

        ].empty:
            
            return 0

        return 1

    def find_games_started_last_5(curr_date, player_id, season_game_logs):

        player_game_logs = season_game_logs[
            (season_game_logs['PLAYER_ID'] == player_id) &
            (season_game_logs['GAME_DATE'] < curr_date) &
            (season_game_logs['MIN'] > 0)
        ]

        player_game_logs = player_game_logs.sort_values("GAME_DATE", ascending=False).iloc[:5]

        if player_game_logs.empty:

            return np.nan

        return len(player_game_logs[
            (player_game_logs['STARTER'] == 1)
        ])

    def find_games_played_last_5_10_compared_to_team(curr_date, player_id, season_game_logs):

        player_logs = season_game_logs[

            (season_game_logs['PLAYER_ID'] == player_id) &
            (season_game_logs['GAME_DATE'] < str(curr_date)) &
            (season_game_logs['MIN'] > 0)

        ].sort_values("GAME_DATE", ascending=False).iloc[:10]

        if player_logs.empty:

            return np.nan, np.nan

        team_id = player_logs['TEAM_ID'].iloc[0]

        team_logs = season_game_logs[

            (season_game_logs['TEAM_ID'] == team_id) &
            (season_game_logs['GAME_DATE'] < str(curr_date))

        ].sort_values("GAME_DATE", ascending=False).drop_duplicates("GAME_DATE").iloc[:10]

        games_played_last_5 = 0
        games_played_last_10 = 0
        team_logs_game_id_list = team_logs['GAME_ID'].to_list()
        player_logs_game_id_list = player_logs['GAME_ID'].to_list()

        i = 0

        for gameId in team_logs_game_id_list:
            
            if gameId in player_logs_game_id_list:

                if games_played_last_5 < 5:

                    games_played_last_5 += 1
                
                games_played_last_10 += 1
                
            i += 1

            if i == 10:

                break
        
        return games_played_last_5, games_played_last_10

    def find_actual_minutes_played(curr_date, player_id, season_game_logs):

        player_game_logs = season_game_logs[
            (season_game_logs['PLAYER_ID'] == player_id) &
            (season_game_logs['GAME_DATE'] == str(curr_date))
        ]

        if player_game_logs.empty:

            return np.nan

        return int(player_game_logs['MIN'].iloc[0])
    
    season_game_logs = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_DATE >= ? ORDER BY GAME_DATE DESC", conn, params=(season_start_date,))
    scoreboard = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER", conn)
    positions_df = pd.read_sql_query("SELECT * FROM PLAYER_POSITIONS", conn)

    latest_date_str = pd.read_sql_query("SELECT * FROM MINUTES_PROJECTION_TRAINING ORDER BY GAME_DATE DESC LIMIT 1", conn)['GAME_DATE'].iloc[0]
    curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
    end_date = datetime.now(ZoneInfo("America/New_York")).date()
    cursor = conn.cursor()

    while curr_date < end_date:
        
        print(f"Updating minutes projection features table for {curr_date}...")

        curr_scoreboard = scoreboard[scoreboard['date'] == str(curr_date)]

        curr_player_ids = curr_scoreboard.drop_duplicates("PLAYER_ID")['PLAYER_ID'].to_list()
        
        for player_id in curr_player_ids:

            positions = positions_df[positions_df['PLAYER_ID'] == player_id]['POSITION'].to_list()
            team_id = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['TeamID'].iloc[0]
            game_id = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['GAME_ID'].iloc[0]
            player_name = curr_scoreboard[curr_scoreboard['PLAYER_ID'] == player_id]['PLAYER'].iloc[0]

            average_last_3, average_last_5, average_last_7, average_last_10 = avg_last_3_5_7_10(season_game_logs, player_id, str(curr_date))
            minute_trend = minutes_trend_5(str(curr_date), player_id, player_name, season_game_logs)
            position_missing_minutes = find_position_missing_minutes(conn, str(curr_date), positions, int(team_id))
            last_10_std_dev = find_last_10_std_dev(str(curr_date), player_id, player_name, season_game_logs)
            days_rest = find_days_rest(str(curr_date), player_id, season_game_logs, season_start_date)
            total_games_played_this_season = find_total_games_played_this_season(str(curr_date), player_id, season_game_logs)
            is_back_to_back = find_if_back_to_back(str(curr_date), player_id, season_game_logs)
            games_started_last_5 = find_games_started_last_5(str(curr_date), player_id, season_game_logs)
            games_played_last_5, games_played_last_10 = find_games_played_last_5_10_compared_to_team(str(curr_date), player_id, season_game_logs)
            actual_minutes_played = find_actual_minutes_played(curr_date, player_id, season_game_logs)

            stats = [
                str(curr_date),
                game_id,
                player_id,
                player_name,
                average_last_3, 
                average_last_5, 
                average_last_7, 
                average_last_10, 
                minute_trend, 
                position_missing_minutes, 
                last_10_std_dev,
                days_rest,
                total_games_played_this_season,
                is_back_to_back,
                games_started_last_5,
                games_played_last_5,
                games_played_last_10,
                actual_minutes_played
            ]

            placeholders = ", ".join(["?"]*len(stats))

            cursor.execute(f"""

                INSERT OR REPLACE INTO MINUTES_PROJECTION_TRAINING VALUES ({placeholders})

            """, stats)
        

        curr_date += timedelta(days=1)

    conn.commit()

if __name__ == "__main__":

    config = load_config()
    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    cursor = conn.cursor()

    curr_date = datetime.strptime("2025-10-21", "%Y-%m-%d").date()
    end_date = datetime.now(ZoneInfo(config.TIMEZONE)).date()
    end_date = datetime.strptime("2026-01-15", "%Y-%m-%d").date()

    update_minutes_projection_features_table(conn=conn, season_start_date=config.SEASON_START_DATE)

    print(f"Done with updating.")