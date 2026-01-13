import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import json
from io import StringIO
import sys
import unicodedata
import time
from zoneinfo import ZoneInfo

from nba_api.stats.endpoints import playbyplayv3, boxscoreusagev3, GameRotation
from nba_api.live.nba.endpoints import boxscore

from basketball_stats_bot.config import load_config

[
 ["Anthony Black", 1641710], ["Ayo Dosunmu", 1630245],
 ["Coby White", 1629632], ["Colin Castleton", 1630658],
 ["Dalen Terry", 1631207], ["Desmond Bane", 1630217],
 ["Emanuel Miller", 1641801],["Franz Wagner",1630532],
 ["Goga Bitadze", 1629048],["Isaac Okoro", 1630171],
 ["Jalen Smith", 1630188],["Jalen Suggs", 1630591],
 ["Jamal Cain", 1631288],["Jase Richardson", 1642859],
 ["Jett Howard", 1641724],["Jevon Carter", 1628975],
 ["Jonathan Isaac", 1628371],["Josh Giddey", 1630581],
 ["Julian Phillips", 1641763],["Kevin Huerter", 1628989],
 ["Lachlan Olbrich", 1642950],["Matas Buzelis", 1641824],
 ["Moritz Wagner", 1629021],["Nikola Vu\u010devi\u0107", 202696],
 ["Noa Essengue", 1642855],["Noah Penda", 1642869],
 ["Orlando Robinson", 1631115],["Paolo Banchero", 1631094],
 ["Patrick Williams", 1630172],["Tre Jones", 1630200],
 ["Trentyn Flowers", 1642280],["Tristan da Silva", 1641783],
 ["Tyus Jones", 1626145],["Wendell Carter Jr.", 1628976],
 ["Zach Collins", 1628380]
]

def find_team_totals_per_player_df(game_id, game_box_score):

    def find_play_by_play_stats_with_dict(play_by_play, on_court_stats_df, game_rotation):

        court_stats_df_to_dict = on_court_stats_df.to_dict(orient='records')            
        subbed_times = set()
        cache = set()

        for team in game_rotation:

            for subbed_out_time in team['IN_TIME_REAL'].to_list():

                subbed_times.add(subbed_out_time)
    
        curr_points_total = 0

        court_stats_dict = {}

        on_court_set = set()

        for player in court_stats_df_to_dict:

            court_stats_dict[player['PLAYER_ID']] = player
        
        play_by_play = play_by_play.to_dict(orient='records')

        court_start_times = {
                                1: 7200.0,
                                2: 14400.0,
                                3: 21600.0,
                                4: 28800.0,
                                5: 31800.0,
                                6: 34800.0,
                                7: 37800.0,
                                8: 40800.0
                            }

        for play in play_by_play:

            quarter = play['period']

            curr_time = play['clock']
            minute_to_tenth_second = float(curr_time[2:4])*600
            tenth_second = float(str(curr_time[5:7]) + str(curr_time[-3:-1]))*0.1
            curr_in_time_real = court_start_times[quarter] - (minute_to_tenth_second + tenth_second)

            print(f"Finding stats for play {play['actionNumber']}...")

            if curr_in_time_real in subbed_times and curr_in_time_real not in cache:

                for team in game_rotation:

                    team_id = float(team['TEAM_ID'].iloc[0])
                    
                    subbed_in = team[team['IN_TIME_REAL'] == curr_in_time_real]
                    player_ids = subbed_in['PERSON_ID'].to_list()

                    for player_id in player_ids:

                        print(f"{player_id} was subbed in.")
                        on_court_set.add((player_id, team_id))

                    subbed_out = team[team['OUT_TIME_REAL'] == curr_in_time_real]
                    player_ids = subbed_out['PERSON_ID'].to_list()

                    for player_id in player_ids:
                        
                        print(f"{player_id} was subbed out.")
                        on_court_set.discard((player_id, team_id))
                
                if len(on_court_set) > 10:

                    return pd.DataFrame()
                
                cache.add(curr_in_time_real)
            
            curr_team_id = play['teamId']

            if play['isFieldGoal'] == 1:

                if play['shotValue'] == 3:

                    if play['shotResult'] == "Made":

                        if play['description'][-4:-1] == 'AST':
                            
                            for pid, curr_player_team_id in on_court_set:
                                
                                if curr_team_id == curr_player_team_id:

                                    court_stats_dict[pid]['AST'] += 1
                                    court_stats_dict[pid]['PTS_AST'] += 4
                                    court_stats_dict[pid]['REB_AST'] += 1
                                    court_stats_dict[pid]['FGA'] += 1
                                    court_stats_dict[pid]['FG3A'] += 1
                                    court_stats_dict[pid]['FG3M'] += 1
                                    court_stats_dict[pid]['FGM'] += 1
                                    court_stats_dict[pid]['PTS'] += 3
                                    court_stats_dict[pid]['PRA'] += 4
                                    court_stats_dict[pid]['PTS_REB'] += 3
                                    court_stats_dict[pid]['PTS_AST'] += 4
                        
                        else:

                            for pid, curr_player_team_id in on_court_set:

                                if curr_team_id == curr_player_team_id:

                                    court_stats_dict[pid]['FGA'] += 1
                                    court_stats_dict[pid]['FG3A'] += 1
                                    court_stats_dict[pid]['FG3M'] += 1
                                    court_stats_dict[pid]['FGM'] += 1
                                    court_stats_dict[pid]['PTS'] += 3
                                    court_stats_dict[pid]['PRA'] += 3
                                    court_stats_dict[pid]['PTS_REB'] += 3
                                    court_stats_dict[pid]['PTS_AST'] += 3

                        curr_points_total += 3
                    
                    elif play['shotResult'] == 'Missed':

                        for pid, curr_player_team_id in on_court_set:
                            
                            if curr_team_id == curr_player_team_id:

                                court_stats_dict[pid]['FGA'] += 1
                                court_stats_dict[pid]['FG3A'] += 1
                
                elif play['shotValue'] == 2:

                    if play['shotResult'] == "Made":

                        if play['description'][-4:-1] == 'AST':
                            
                            for pid, curr_player_team_id in on_court_set:

                                if curr_team_id == curr_player_team_id:

                                    court_stats_dict[pid]['AST'] += 1
                                    court_stats_dict[pid]['PTS_AST'] += 3
                                    court_stats_dict[pid]['REB_AST'] += 1
                                    court_stats_dict[pid]['FGA'] += 1
                                    court_stats_dict[pid]['FGM'] += 1
                                    court_stats_dict[pid]['PTS'] += 2
                                    court_stats_dict[pid]['PRA'] += 3
                                    court_stats_dict[pid]['PTS_REB'] += 2
                                    court_stats_dict[pid]['PTS_AST'] += 3

                        else:

                            for pid, curr_player_team_id in on_court_set:

                                if curr_team_id == curr_player_team_id:

                                    court_stats_dict[pid]['FGA'] += 1
                                    court_stats_dict[pid]['FGM'] += 1
                                    court_stats_dict[pid]['PTS'] += 2
                                    court_stats_dict[pid]['PRA'] += 2
                                    court_stats_dict[pid]['PTS_REB'] += 2
                                    court_stats_dict[pid]['PTS_AST'] += 2

                        curr_points_total += 2
                    
                    elif play['shotResult'] == 'Missed':

                        for pid, curr_player_team_id in on_court_set:
                            
                            if curr_team_id == curr_player_team_id:
                                
                                court_stats_dict[pid]['FGA'] += 1

            elif play['actionType'] == 'Free Throw':

                if play['pointsTotal'] > curr_points_total:

                    for pid, curr_player_team_id in on_court_set:
                        
                        if curr_team_id == curr_player_team_id:

                            court_stats_dict[pid]['FTA'] += 1                    
                            court_stats_dict[pid]['FTM'] += 1 
                            court_stats_dict[pid]['PTS'] += 1                   
                            court_stats_dict[pid]['PRA'] += 1                    
                            court_stats_dict[pid]['PTS_REB'] += 1                    
                            court_stats_dict[pid]['PTS_AST'] += 1

                    curr_points_total += 1
    
                else:

                    for pid, curr_player_team_id in on_court_set:

                        if curr_team_id == curr_player_team_id:

                            court_stats_dict[pid]['FTA'] += 1

            elif play['actionType'] == 'Rebound':

                for pid, curr_player_team_id in on_court_set:

                    if curr_team_id == curr_player_team_id:

                        court_stats_dict[pid]['REB'] += 1
                        court_stats_dict[pid]['PRA'] += 1
                        court_stats_dict[pid]['PTS_REB'] += 1
                        court_stats_dict[pid]['REB_AST'] += 1

            elif 'STEAL' in play['description']:
                
                for pid, curr_player_team_id in on_court_set:

                    if curr_team_id == curr_player_team_id:

                        court_stats_dict[pid]['STL'] += 1

            elif 'BLOCK' in play['description']:

                for pid, curr_player_team_id in on_court_set:

                    if curr_team_id == curr_player_team_id:

                        court_stats_dict[pid]['BLK'] += 1   

        dfs = []

        for pid, player_dict in court_stats_dict.items():

            dfs.append(pd.DataFrame([player_dict]))
        
        return pd.concat(dfs, ignore_index=True)
    
    play_by_play = playbyplayv3.PlayByPlayV3(game_id=game_id).get_data_frames()[0]
    time.sleep(2)

    game_rotation = []

    player_ids = game_box_score['PLAYER_ID'].to_list()

    curr_date = game_box_score['GAME_DATE'].iloc[0]

    dfs = []
    for pid in player_ids:

        curr_team_id = game_box_score[game_box_score['PLAYER_ID'] == pid]['TEAM_ID'].iloc[0]
        player_name = game_box_score[game_box_score['PLAYER_ID'] == pid]['PLAYER_NAME'].iloc[0]

        dfs.append(pd.DataFrame([{
            'GAME_DATE': curr_date,
            'GAME_ID': game_id,
            'TEAM_ID': int(curr_team_id),
            'PLAYER_ID': pid,
            'PLAYER_NAME': player_name
        }]))

    rosters = pd.concat(dfs, ignore_index=True)

    col_names = ['GAME_DATE', 'GAME_ID', 'PLAYER_NAME', 'PLAYER_ID', 'PTS', 'REB', 'AST', 'STL', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'BLK', 'PRA', 'PTS_REB', 'PTS_AST', 'REB_AST']

    on_court_stats_df = pd.DataFrame(columns=col_names)

    on_court_stats_df = pd.concat([on_court_stats_df, rosters], ignore_index=True)

    # finds all cells where it doesn't equal NaN, and replaces 0 with all the other cells
    on_court_stats_df = on_court_stats_df.where(on_court_stats_df.notna(), 0)
    on_court_stats_df = find_play_by_play_stats_with_dict(play_by_play, on_court_stats_df, game_rotation)

    if on_court_stats_df.empty:

        return pd.DataFrame()

    on_court_stats_df = on_court_stats_df.sort_values("PLAYER_NAME").reset_index(drop=True)

    return on_court_stats_df

if __name__ == "__main__":

    
    config = load_config()
    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()

    game_id = "0022500314"

    game_rotation = GameRotation(game_id=game_id).get_data_frames()[0]

    print(game_rotation)
    sys.exit(1)

    curr_box_score = pd.read_sql_query("SELECT * FROM player_game_logs WHERE GAME_ID = ?", conn, params=(game_id,))
    curr_df = find_team_totals_per_player_df(game_id=game_id, game_box_score=curr_box_score)
        

    curr_df = curr_df.drop(columns=['TEAM_ID'])

    curr_df = curr_df.to_dict(orient='records')

    for hashmap in curr_df:

        placeholders = ", ".join(['?']*len(hashmap))
        col_names = ", ".join(list(hashmap.keys()))

        cursor.execute(f"""

            INSERT OR REPLACE INTO TEAM_TOTALS_PER_PLAYER ({col_names})
            VALUES ({placeholders})

        """, list(hashmap.values()))

    conn.commit()

    print(f"Finished updating team totals per player sql table for {game_id}")

    
    
