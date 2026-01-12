from nba_api.live.nba.endpoints import boxscore
import json
import time
import os
import pandas as pd
import sys
import sqlite3
from datetime import date, datetime, timedelta
import unicodedata

from basketball_stats_bot.config import load_config


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

    file_path = os.path.join(config.GAME_FILES_PATH, date, "system_grade.json")
    system.to_json(file_path, orient="records", indent=4)

    return system

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_PATH)
    system_df = pd.read_sql_query("SELECT * FROM SYSTEM", conn)

    non_updated = system_df[system_df['RESULT'].isna()]

    if non_updated.empty:

        print("System is all up to date")
        sys.exit(1)
    
    non_updated.sort_values("DATE", ascending=True).drop_duplicates("DATE")

    curr_date_str = non_updated['DATE'].iloc[0]
    end_date_str = date.today()
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    while curr_date <= end_date:

        df = system_grade(str(curr_date), conn)

        df = df[df["SCORE"] >= 70]

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

            text = f"\nThere were a total of {total_length} props that had a score above 70 during {str(curr_date)}."
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
    
    print(f"Finished grading systems from {curr_date_str} - {end_date_str}")
