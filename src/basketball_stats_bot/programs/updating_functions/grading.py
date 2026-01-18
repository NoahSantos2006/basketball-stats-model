from nba_api.live.nba.endpoints import boxscore
import json
import time
import os
import pandas as pd
import sys
import sqlite3
from datetime import date, datetime, timedelta
import unicodedata
from zoneinfo import ZoneInfo

from basketball_stats_bot.config import load_config

def update_system(conn):

    def system_grade(date, conn):
    
        cursor = conn.cursor()

        today_nba_api_game_ids = pd.read_sql_query("SELECT * FROM NBA_API_GAME_IDS WHERE DATE = ?", conn, params=(str(date),))

        if today_nba_api_game_ids.empty:

            print(f"No games found for {date}")
            return pd.DataFrame()
        
        else:

            today_nba_api_game_ids = today_nba_api_game_ids['GAME_ID'].to_list()

        allteamboxscores = []

        for gameId in today_nba_api_game_ids:

            print(f"Finding the boxscore for {gameId}...")
            
            try:
                box = boxscore.BoxScore(gameId)
            except Exception as e:
                print(f"Could not find a boxscore for {gameId} ({e})")
                raise

            stats = box.get_dict()['game']

            if stats['gameStatus'] == 3:

                allteamboxscores.append(pd.DataFrame(stats['homeTeam']['players']))
                allteamboxscores.append(pd.DataFrame(stats['awayTeam']['players']))


        today_box_scores = pd.concat(allteamboxscores, ignore_index=True)

        player_ids = today_box_scores['personId'].to_list()

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

        for pid in player_ids:

            curr_player_boxscore = today_box_scores[today_box_scores['personId'] == pid]
            curr_player_status = curr_player_boxscore['status'].iloc[0]

            if curr_player_status == 'INACTIVE':

                cursor.execute("""

                    DELETE FROM SYSTEM
                    WHERE DATE = ?
                    AND PLAYER_ID = ?

                """, (str(date), pid))

                continue
            
            currStats = curr_player_boxscore['statistics'].iloc[0]
            currLines = system[system['PLAYER_ID'] == pid]
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
                    AND PLAYER_ID = ?
                    AND PROP = ?
                            
                    """, (result, date, pid, prop))

        conn.commit()

        system = system[

            (system['RESULT'] == 1) |
            (system['RESULT'] == 0)
            
        ].copy()

        date_dir_path = os.path.join(config.GAME_FILES_PATH, date,)

        if not os.path.isdir(date_dir_path):

            os.mkdir(date_dir_path)

        file_path = os.path.join(config.GAME_FILES_PATH, date, "system_grade.json")

        system.to_json(file_path, orient="records", indent=4)

        return system

    config = load_config()

    system_df = pd.read_sql_query("SELECT * FROM SYSTEM", conn)

    today = datetime.now(ZoneInfo(config.TIMEZONE)).strftime("%Y-%m-%d")

    non_updated = system_df[
        (system_df['RESULT'].isna()) &
        (system_df['DATE'] != str(today))
    ]

    if non_updated.empty:

        print("System is all up to date")

        ui = input("Do you want to grade the system today? (y/n): ").lower()

        while ui not in {'y', 'n'}:

            ui = input("Do you want to grade the system today? (y/n): ").lower()

        if ui == 'n':

            return -1

    non_updated = non_updated.sort_values("DATE", ascending=True).drop_duplicates("DATE")

    if non_updated.empty:

        curr_date_str = str(date.today())
        curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
        
    else:

        curr_date_str = non_updated['DATE'].iloc[0]
        curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()

    end_date = datetime.now(ZoneInfo(config.TIMEZONE)).date()

    while curr_date <= end_date:

        df = system_grade(str(curr_date), conn)

        if df.empty:

            return

        df = df[df["SCORE"] >= 60]

        if len(df) >= 10:
            
            last_10 = df.iloc[:10]
            last_10_hit = len(last_10[last_10['RESULT'] == 1])

        if len(df) >= 20:

            last_20 = df.iloc[:20]
            last_20_hit = len(last_20[last_20['RESULT'] == 1][:20])

        if len(df) >= 50:

            last_50 = df.iloc[:50]
            last_50_hit = len(last_50[last_50['RESULT'] == 1][:50])

        if len(df) >= 100:

            last_100 = df.iloc[:100]
            last_100_hit = len(last_100[last_100['RESULT'] == 1][:100])

        overall_hit = len(df[df['RESULT'] == 1])

        total_length = len(df)
        
        text_file_path = os.path.join(config.GAME_FILES_PATH, str(curr_date), "grade.txt")

        with open(text_file_path, "w") as f:

            text = f"\nThere were a total of {total_length} props that had a score above 60 during {str(curr_date)}."
            f.write(text)
            print(text)

        if len(df) >= 10:

            with open(text_file_path, "a") as f:
                text = f"\nIn the top 10 props you went {(last_10_hit / 10)*100:.2f}%\n"
                f.write(f"\n{text}")
                print(text)

        if len(df) >= 20:

            with open(text_file_path, "a") as f:
                text = f"In the top 20 props you went {(last_20_hit / 20)*100:.2f}%\n"
                f.write(f"\n{text}")
                print(text)

        if len(df) >= 50:

            with open(text_file_path, "a") as f:
                text = f"In the top 50 props you went {(last_50_hit / 50*100):.2f}%\n"
                f.write(f"\n{text}")
                print(text)

        if len(df) >= 100:
            
            with open(text_file_path, "a") as f:
                text = f"In the top 100 props you went {(last_100_hit / 100)*100:.2f}%\n"
                f.write(f"\n{text}")
                print(text)

        with open(text_file_path, "a") as f:
            
            text = f"In {total_length} props you went {(overall_hit / total_length)*100:.2f}%\n"
            f.write(f"\n{text}")
            print(text)
        
        curr_date += timedelta(days=1)
    
    print(f"Finished grading systems from {curr_date_str} - {end_date}")

def grade_system(conn, curr_date_str, end_date_str):

    def system_grade(date, conn):
    
        cursor = conn.cursor()

        today_nba_api_game_ids = pd.read_sql_query("SELECT * FROM NBA_API_GAME_IDS WHERE DATE = ?", conn, params=(str(date),))

    
        if today_nba_api_game_ids.empty:

            print(f"No games found for {date}")
            return pd.DataFrame()
        
        else:

            today_nba_api_game_ids = today_nba_api_game_ids['GAME_ID'].to_list()

        allteamboxscores = []

        for gameId in today_nba_api_game_ids:

            print(f"Finding the boxscore for {gameId}...")
            
            try:

                box = boxscore.BoxScore(gameId)

            except Exception as e:
                print(f"Could not find a boxscore for {gameId} ({e})")
                sys.exit(1)

            stats = box.get_dict()['game']

            if stats['gameStatus'] == 3:

                allteamboxscores.append(pd.DataFrame(stats['homeTeam']['players']))
                allteamboxscores.append(pd.DataFrame(stats['awayTeam']['players']))

        today_box_scores = pd.concat(allteamboxscores, ignore_index=True)

        player_ids = today_box_scores['personId'].to_list()

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

        for pid in player_ids:

            curr_player_boxscore = today_box_scores[today_box_scores['personId'] == pid]
            curr_player_status = curr_player_boxscore['status'].iloc[0]

            if curr_player_status == 'INACTIVE':

                cursor.execute("""

                    DELETE FROM SYSTEM
                    WHERE DATE = ?
                    AND PLAYER_ID = ?

                """, (str(date), pid))

                continue
            
            currStats = curr_player_boxscore['statistics'].iloc[0]
            currLines = system[system['PLAYER_ID'] == pid]
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
                    AND PLAYER_ID = ?
                    AND PROP = ?
                            
                    """, (result, date, pid, prop))

        conn.commit()

        system = system[

            (system['RESULT'] == 1) |
            (system['RESULT'] == 0)
            
        ].copy()

        date_dir_path = os.path.join(config.GAME_FILES_PATH, date,)

        if not os.path.isdir(date_dir_path):

            os.mkdir(date_dir_path)

        file_path = os.path.join(config.GAME_FILES_PATH, date, "system_grade.json")

        system.to_json(file_path, orient="records", indent=4)

        return system

    config = load_config()

    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    while curr_date <= end_date:

        df = system_grade(str(curr_date), conn)

        if df.empty:

            return

        df = df[df["SCORE"] >= 60]

        if len(df) >= 10:
            
            last_10 = df.iloc[:10]
            last_10_hit = len(last_10[last_10['RESULT'] == 1])

        if len(df) >= 20:

            last_20 = df.iloc[:20]
            last_20_hit = len(last_20[last_20['RESULT'] == 1][:20])

        if len(df) >= 50:

            last_50 = df.iloc[:50]
            last_50_hit = len(last_50[last_50['RESULT'] == 1][:50])

        if len(df) >= 100:

            last_100 = df.iloc[:100]
            last_100_hit = len(last_100[last_100['RESULT'] == 1][:100])

        overall_hit = len(df[df['RESULT'] == 1])

        total_length = len(df)
        
        text_file_path = os.path.join(config.GAME_FILES_PATH, str(curr_date), "grade.txt")

        with open(text_file_path, "w") as f:

            text = f"\nThere were a total of {total_length} props that had a score above 60 during {str(curr_date)}."
            f.write(text)
            print(text)

        if len(df) >= 10:

            with open(text_file_path, "a") as f:
                text = f"\nIn the top 10 props you went {(last_10_hit / 10)*100:.2f}%\n"
                f.write(f"\n{text}")
                print(text)

        if len(df) >= 20:

            with open(text_file_path, "a") as f:
                text = f"In the top 20 props you went {(last_20_hit / 20)*100:.2f}%\n"
                f.write(f"\n{text}")
                print(text)

        if len(df) >= 50:

            with open(text_file_path, "a") as f:
                text = f"In the top 50 props you went {(last_50_hit / 50*100):.2f}%\n"
                f.write(f"\n{text}")
                print(text)

        if len(df) >= 100:
            
            with open(text_file_path, "a") as f:
                text = f"In the top 100 props you went {(last_100_hit / 100)*100:.2f}%\n"
                f.write(f"\n{text}")
                print(text)

        with open(text_file_path, "a") as f:
            
            text = f"In {total_length} props you went {(overall_hit / total_length)*100:.2f}%\n"
            f.write(f"\n{text}")
            print(text)
        
        curr_date += timedelta(days=1)
    
    print(f"Finished grading systems from {curr_date_str} - {end_date}")

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    start_date_str = "2026-01-16"
    end_date_str = "2026-01-16"

    grade_system(conn=conn, curr_date_str=start_date_str, end_date_str=end_date_str)
