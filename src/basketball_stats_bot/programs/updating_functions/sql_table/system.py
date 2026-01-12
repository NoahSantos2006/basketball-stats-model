import pandas as pd
import json
import sys
from datetime import datetime, date, timedelta
import pandas as pd
import sqlite3
import time
import os
from io import StringIO
import unicodedata
from nba_api.live.nba.endpoints import boxscore
from zoneinfo import ZoneInfo
import numpy as np

from basketball_stats_bot.config import load_config
from basketball_stats_bot.programs.scoring.scoring_functions import scoringv6

def update_system_table(conn):

    def clean_name(text):

            removed_accents_text =  "".join(
                c for c in unicodedata.normalize('NFD', text)
                if unicodedata.category(c) != "Mn"
            )

            clean = removed_accents_text.replace(".", "")

            return clean

    def player_vs_prop_scores(player_vs_team_or_last_20_df, draftkings_sportsbook, date, conn):
        
        translation = {

            "player_points": "PTS",
            "player_rebounds": "REB",
            "player_assists": "AST",
            "player_threes": "FG3M",
            "player_blocks": "BLK",
            "player_steals": "STL",
            "player_blocks_steals": "BLK_STL",
            "player_turnovers": "TOV",
            "player_points_rebounds_assists": "PRA",
            "player_points_rebounds": "PTS_REB",
            "player_points_assists": "PTS_AST",
            "player_rebounds_assists": "REB_AST",
            "player_field_goals": "FGM",
            "player_frees_made": "FTM",
            "player_frees_attempt": "FGA"

        }

        name_edge_cases = {
            'Carlton Carrington': 'Bub Carrington',
            'Isaiah Stewart II': 'Isaiah Stewart',
            'Nicolas Claxton': 'Nic Claxton',
            'Jimmy Butler': 'Jimmy Butler III',
            'Marvin Bagley': 'Marvin Bagley III',
            'Ronald Holland': 'Ronald Holland II',
            'DaRon Holmes': 'DaRon Holmes II',
            'Trey Jemison': 'Trey Jemison III',
            'Derrick Lively': 'Derrick Lively II',
            'Trey Murphy': 'Trey Murphy III',
            'Gary Payton': 'Gary Payton II',
            'Lindy Waters': 'Lindy Waters III',
            'Robert Williams': 'Robert Williams III',
            'Vincent Williams Jr': 'Vince Williams Jr',
            'Ron Holland': 'Ronald Holland II',
            'Herb Jones': 'Herbert Jones',
            'Derrick Jones': 'Derrick Jones Jr'
        }

        system = {}
        cursor = conn.cursor()
        conn.create_function("clean_name", 1, clean_name)

        scoreboard = pd.read_sql_query("SELECT date, PLAYER, opposition_team_id FROM SCOREBOARD_TO_ROSTER", conn)
        scoreboard['PLAYER'] = scoreboard['PLAYER'].apply(clean_name)

        player_games = {
            player: df for player, df in player_vs_team_or_last_20_df.groupby("NAME_CLEAN")
        }

        system = {}

        for player, prop_lines in draftkings_sportsbook.items():

            print(f"Calculating score for {player}")

            if player in name_edge_cases:

                player = name_edge_cases[player]

            if player not in system:

                system[player] = {}

            if player not in player_games:

                print(f"Could not find {player} in player_games")
                sys.exit(1)
                continue

            curr = player_games[player]

            if curr.empty:

                continue
            
            
            current_opposition_id = scoreboard[scoreboard["PLAYER"] == player]['opposition_team_id'].iloc[0]

            for prop, line in prop_lines.items():
                
                curr_score = scoringv6(curr, current_opposition_id, translation[prop], line)

                system[player][prop] = (curr_score, line)
                system[player]['PERSON_ID'] = int(curr['PLAYER_ID'].iloc[0])

        return system

    def result(scores, date, conn):
            
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

            def insert_reverse_sorted(self, player_name, prop, score, line, matchup, curr_date, player_id):

                if score < 0:

                    over_under = "U"
                
                elif score > 0:

                    over_under = "O"
                
                else:

                    over_under = "O/U"
                
                now = time.strftime(f"%Y-%m-%d %H:%M:%S")

                node = Node({
                    'DATE': curr_date,
                    "PLAYER": player_name,
                    "OVER_UNDER": over_under, 
                    "PROP": prop, 
                    "LINE": line, 
                    "MATCHUP": matchup,
                    "SCORE": abs(score),
                    "LAST_UPDATED": now,
                    "PLAYER_ID": player_id,
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

        result_linked_list = LinkedList()

        for player_name, score_dict in scores.items():
            
            print(f"Adding {player_name} to the system..")

            for prop, score in score_dict.items():

                if prop == 'PERSON_ID':
                    continue

                currScore, line = score

                rosters = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE DATE = ?", conn, params=(date,))
                
                curr = rosters[rosters['PLAYER_ID'] == score_dict["PERSON_ID"]]

                matchup = curr['MATCHUP'].to_list()[0]

                result_linked_list.insert_reverse_sorted(player_name, prop, currScore, line, matchup, date, int(score_dict['PERSON_ID']))

        system_sorted = result_linked_list.to_array()

        print(f"Created a system for {date}")

        for node in system_sorted:

            placeholders = ", ".join(['?'] * len(node))

            cursor.execute(f"""

                INSERT OR REPLACE INTO SYSTEM (DATE, PLAYER, OVER_UNDER, PROP, LINE, MATCHUP, SCORE, LAST_UPDATED, PLAYER_ID)
                VALUES ({placeholders})

            """, list(node.values()))
        
        conn.commit()

        return pd.DataFrame(system_sorted)

    def system_grade(date, df, conn): 
        
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM NBA_API_GAME_IDS WHERE DATE = ?", (str(date),))
        fetch = cursor.fetchall()

        today_nba_api_game_ids = [gameId for curr_date, gameId in fetch]

        allteamboxscores = []

        for gameId in today_nba_api_game_ids:

            box = boxscore.BoxScore(gameId)

            stats = box.get_dict()['game']

            allteamboxscores.append(pd.DataFrame(stats['homeTeam']['players']))
            allteamboxscores.append(pd.DataFrame(stats['awayTeam']['players']))


        today_box_scores = pd.concat(allteamboxscores, ignore_index=True)

        player_names = today_box_scores['name'].to_list()

        df["RESULT"] = pd.NA

        translation = {
            "player_points": 'points',
            "player_rebounds": 'reboundsTotal',
            "player_assists": "assists",
            "player_threes": "threePointersMade",
            "player_blocks": "blocks",
            "player_steals": "steals",
        }

        extra = {
            "player_points_rebounds_assists",
            "player_points_rebounds",
            "player_points_assists",
            "player_rebounds_assists",
        }

        for player in player_names:

            print(f"Grading {player}...")

            currStats = today_box_scores[today_box_scores['name'] == player]['statistics'].iloc[0]

            currLines = df[df['PLAYER'] == clean_name(player)]

            props = currLines['PROP'].to_list()

            for prop in props:
                
                if prop == "player_points_rebounds_assists":

                    curr_line = currStats['points'] + currStats['reboundsTotal'] + currStats['assists']

                elif prop == "player_points_rebounds":

                    curr_line = currStats['points'] + currStats['reboundsTotal']

                elif prop == "player_points_assists":

                    curr_line = currStats['points'] + currStats['assists']

                elif prop == "player_rebounds_assists":

                    curr_line = currStats['reboundsTotal'] + currStats['assists']
                
                else:

                    curr_line = currStats[translation[prop]]

                # finds the index of where the prop is
                idx = currLines.index[currLines['PROP'] == prop].to_list()[0]

                # finds the prop
                comparison_stat = currLines[currLines['PROP'] == prop]

                # finds prop line and whether it should be over/under
                over_under = comparison_stat['OVER_UNDER'].iloc[0]
                prop_line = comparison_stat['LINE'].iloc[0]

                if over_under == "O":
                    
                    if prop_line < curr_line:
                        
                        result_binary = 1
                        df.loc[idx, 'RESULT'] = 1
                    
                    else:

                        result_binary = 0
                        df.loc[idx, 'RESULT'] = 0
                
                if over_under == "U":
                    
                    if prop_line > curr_line:
                        
                        result_binary = 1
                        df.loc[idx, 'RESULT'] = 1

                    else:

                        result_binary = 0
                        df.loc[idx, 'RESULT'] = 0

                cursor.execute("""

                    UPDATE SYSTEM
                    SET RESULT = ?
                    WHERE DATE = ?
                    AND PLAYER = ?
                    AND PROP = ?
                            
                    """, (result_binary, str(date), clean_name(player), prop))

                conn.commit()

        # this is if the score was 0 then the over_under category = O/U because it could literally go either way
        
        df = df[

            (df['RESULT'] == 1) |
            (df['RESULT'] == 0)
            
        ].copy()

        return df

    conn.create_function('clean_name', 1, clean_name)
    cursor = conn.cursor()

    latest_date_str = pd.read_sql_query("SELECT * FROM SYSTEM WHERE RESULT = ? ORDER BY GAME_DATE DESC", conn, params=(np.nan,))['GAME_DATE'].iloc[0]
    end_date = datetime.now(ZoneInfo(config.TIMEZONE)).date()
    curr_date = datetime.strptime(latest_date_str, '%Y-%m-%d').date()

    translation = {

        'PTS': 'player_points',
        'REB': 'player_rebounds',
        'AST': 'player_assists',
        'FG3M': 'player_threes',
        'BLK': 'player_blocks',
        'STL': 'player_steals',
        'PRA': 'player_points_rebounds_assists',
        'PTS_REB': 'player_points_rebounds',
        'PTS_AST': 'player_points_assists',
        'REB_AST': 'player_rebounds_assists'

    }

    while curr_date <= end_date:

        print(f'Updating sql table SYSTEM for {str(curr_date)}..')

        cursor.execute("SELECT * FROM PLAYER_VS_TEAM_OR_LAST_20_JSONS WHERE DATE = ?", (str(curr_date),))

        fetch = cursor.fetchall()

        if not fetch:

            print(f"Couldn't find player_vs_team_or_last_20 json file for {str(curr_date)}")
            curr_date += timedelta(days=1)
            continue

        # use StringIO because 'read_json' is depcrecated
        player_vs_team_or_last_20_df = pd.read_json(StringIO(fetch[0][1]))

        player_props = pd.read_sql_query("SELECT * FROM PLAYER_PROPS WHERE DATE = ?", conn, params=(str(curr_date),))

        player_names = player_props['PLAYER'].to_list()

        draftkings_sportsbook = {}

        for player in player_names:

            curr = player_props[player_props["PLAYER"] == player].to_dict()

            # drops all player_lines that aren't given by the sportsbook
            curr = {k: v for k, v in curr.items() if not pd.isna(list(v.values())[0])}

            draftkings_sportsbook[player.replace(".", "")] = {}

            for col, row in curr.items():

                if col not in {'DATE', 'PLAYER'}:
                    
                    for key, val in row.items():
                        
                        draftkings_sportsbook[player.replace(".", "")][translation[col]] = val
        
        scores = player_vs_prop_scores(player_vs_team_or_last_20_df, draftkings_sportsbook, str(curr_date), conn)

        result_df = result(scores, str(curr_date), conn)

        system_grade(curr_date, result_df, conn)

        curr_date += timedelta(days=1)
    
    print(f"Updated system from {latest_date_str} - {end_date}")

def system_grade(date, conn):

    def clean_name(text):

        removed_accents_text =  "".join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != "Mn"
        )

        clean = removed_accents_text.replace(".", "")

        return clean
    
    cursor = conn.cursor()

    today_nba_api_game_ids = pd.read_sql_query("SELECT * FROM NBA_API_GAME_IDS WHERE DATE = ?", conn, params=(str(date),))

    if today_nba_api_game_ids.empty:

        print("No games found")
        return
    
    else:

        today_nba_api_game_ids = today_nba_api_game_ids['GAME_ID'].to_list()

    allteamboxscores = []

    for gameId in today_nba_api_game_ids:

        print(f"Finding the boxscore for {gameId}...")
        
        box = boxscore.BoxScore(gameId)

        stats = box.get_dict()['game']

        allteamboxscores.append(pd.DataFrame(stats['homeTeam']['players']))
        allteamboxscores.append(pd.DataFrame(stats['awayTeam']['players']))


    today_box_scores = pd.concat(allteamboxscores, ignore_index=True)

    player_names = today_box_scores['name'].to_list()

    system = pd.read_sql_query("SELECT * FROM SYSTEM WHERE DATE = ? ORDER BY SCORE DESC", conn, params=(str(date),))

    system["RESULT"] = pd.NA

    translation = {
        "player_points": 'points',
        "player_rebounds": 'reboundsTotal',
        "player_assists": "assists",
        "player_threes": "threePointersMade",
        "player_blocks": "blocks",
        "player_steals": "steals",
    }

    extra = {
        "player_points_rebounds_assists",
        "player_points_rebounds",
        "player_points_assists",
        "player_rebounds_assists",
    }

    for player in player_names:

        currStats = today_box_scores[today_box_scores['name'] == player]['statistics'].iloc[0]

        # i changed everything from lowercase to capital in a later version
        if 'PLAYER' not in system.columns:

            currLines = system[system['player'] == clean_name(player)]

        else:

            currLines = system[system['PLAYER'] == clean_name(player)]

        if 'prop' not in currLines.columns:
            
            props = currLines['PROP']
        else:
            props = currLines['prop'].to_list()

        for prop in props:
            
            if prop == "player_points_rebounds_assists":

                curr_line = currStats['points'] + currStats['reboundsTotal'] + currStats['assists']

            elif prop == "player_points_rebounds":

                curr_line = currStats['points'] + currStats['reboundsTotal']

            elif prop == "player_points_assists":

                curr_line = currStats['points'] + currStats['assists']

            elif prop == "player_rebounds_assists":

                curr_line = currStats['reboundsTotal'] + currStats['assists']
            
            else:

                curr_line = currStats[translation[prop]]

            if 'PLAYER' not in system.columns:

                idx = currLines.index[currLines['prop'] == prop].to_list()[0]
                comparison_stat = currLines[currLines['prop'] == prop]
                over_under = comparison_stat['over_under'].iloc[0]
                prop_line = comparison_stat['line'].iloc[0]
            
            else:

                idx = currLines.index[currLines['PROP'] == prop].to_list()[0]
                comparison_stat = currLines[currLines['PROP'] == prop]
                over_under = comparison_stat['OVER_UNDER'].iloc[0]
                prop_line = comparison_stat['LINE'].iloc[0]

            if over_under == "O":
                
                if prop_line < curr_line:
                    
                    result = 1
                    system.loc[idx, 'RESULT'] = 1
                
                else:
                    
                    result = 0
                    system.loc[idx, 'RESULT'] = 0
            
            if over_under == "U":
                
                if prop_line > curr_line:
                    
                    result = 1
                    system.loc[idx, 'RESULT'] = 1
                
                else:
                    
                    result = 0
                    system.loc[idx, 'RESULT'] = 0

            cursor.execute("""

                UPDATE SYSTEM
                SET RESULT = ?
                WHERE DATE = ?
                AND PLAYER = ?
                AND PROP = ?
                           
                """, (result, date, clean_name(player), prop))

            conn.commit()

    system = system[

        (system['RESULT'] == 1) |
        (system['RESULT'] == 0)
        
    ].copy()

    file_path = os.path.join(game_data_path, date, "system_grade.json")
    system.to_json(file_path, orient="records", indent=4)

    return system


if __name__ == "__main__":

    config = load_config()

    conn = sqlite3(config.DB_PATH)

