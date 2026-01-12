import json
import time
import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import sqlite3
from zoneinfo import ZoneInfo

from basketball_stats_bot.config import load_config

def result(scores, date, conn):

    config = load_config()
        
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
                'DATE': str(curr_date),
                "PLAYER": player_name, 
                "OVER_UNDER": over_under, 
                "PROP": prop, 
                "LINE": line, 
                "MATCHUP": matchup,
                "SCORE": abs(score),
                "LAST_UPDATED": str(now),
                "PLAYER_ID": player_id
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
        
    system = LinkedList()

    for player_name, score_dict in scores.items():
        
        for prop, score in score_dict.items():

            if prop == 'PERSON_ID':
                continue

            currScore, line = score
            
            rosters = pd.read_sql_query("SELECT * FROM SCOREBOARD_TO_ROSTER WHERE DATE = ?", conn, params=(str(date),))
            
            curr = rosters[rosters['PLAYER_ID'] == score_dict["PERSON_ID"]]

            matchup = curr['MATCHUP'].to_list()[0]

            system.insert_reverse_sorted(player_name, prop, currScore, line, matchup, date, score_dict["PERSON_ID"])

    system_sorted = system.to_array()

    file_path = os.path.join(config.GAME_FILES_PATH, str(date), "system.json")

    with open(file_path, "w") as f:

        json.dump(system_sorted, f, indent=4)
    
    cursor = conn.cursor()

    for node in system_sorted:

        placeholders = ", ".join(['?'] * len(node))

        cursor.execute(f"""

            INSERT OR REPLACE INTO SYSTEM (DATE, PLAYER, OVER_UNDER, PROP, LINE, MATCHUP, SCORE, LAST_UPDATED, PLAYER_ID)
            VALUES ({placeholders})

        """, list(node.values()))
    
    conn.commit()
    print(f"\nDatabase table SYSTEM updated.\n")

    print(f"System was added in: {file_path}\n")

    return system_sorted

if __name__ == "__main__":

    config = load_config()
    
    curr_date_str = '2025-12-11'
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    today = datetime.now(config.TIMEZONE).date()

    conn = sqlite3.connect(config.DB_PATH)

    while curr_date <= today:
        
        scores_path = os.path.join(config.GAME_FILES_PATH, str(curr_date), "scores.json")

        with open(scores_path, "r") as f:

            scores = json.load(f)

        result(scores, str(curr_date), config.GAME_FILES_PATH, conn)

        curr_date += timedelta(days=1)
    
    print(f"System.jsons are updated")

