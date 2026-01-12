import requests
from datetime import date, timedelta, datetime
from nba_api.stats.endpoints import leaguegamefinder
import sqlite3
from nba_api.live.nba.endpoints import scoreboard
from zoneinfo import ZoneInfo

def update_nba_api_game_ids(conn):

    def find_game_ids_by_date(cursor, latest_date_str):
        
        curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
        today = datetime.now(ZoneInfo("America/New_York")).date()

        while curr_date <= today:

            cursor.execute(f"""

                SELECT * FROM NBA_API_GAME_IDS
                WHERE DATE = ?

            """, (str(curr_date),))

            fetch = cursor.fetchall()

            if fetch:

                curr_date += timedelta(days=1)
                continue

            print(f"Finding NBA API game ids for {str(curr_date)}...")

            if curr_date == today:
                
                board = scoreboard.ScoreBoard()
                games = board.games.get_dict()

                for game in games:

                    cursor.execute("""

                        INSERT OR REPLACE INTO NBA_API_GAME_IDS (DATE, GAME_ID)
                        VALUES (?, ?)

                    """, (str(curr_date), game['gameId']))
                
                curr_date += timedelta(days=1)
                
                
            else:

                gamefinder = leaguegamefinder.LeagueGameFinder(
                    date_from_nullable=curr_date,
                    date_to_nullable=curr_date
                )

                games = gamefinder.get_data_frames()[0]

                current_games = list(games['GAME_ID'].drop_duplicates())

                for gameId in current_games:
                    
                    cursor.execute("""

                        INSERT OR REPLACE INTO NBA_API_GAME_IDS (DATE, GAME_ID)
                        VALUES (?, ?)

                    """, (str(curr_date), gameId))
                
                conn.commit()
                
                curr_date += timedelta(days=1)

    cursor = conn.cursor()

    cursor.execute("""

        SELECT *
        FROM NBA_API_GAME_IDS
        ORDER BY DATE DESC
        LIMIT 1

    """)

    fetch = cursor.fetchall()

    latest_date = fetch[0][0]

    # deletes all rows but keeps schema
    # cursor.execute("DELETE FROM NBA_API_GAME_IDS WHERE DATE = '2025-12-20'")
    # conn.commit()
    # exit()


    find_game_ids_by_date(cursor, latest_date)

    conn.commit()
    
    print("NBA_API game ids updated")
    

if __name__ == "__main__":

    conn = sqlite3.connect(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")
    cursor = conn.cursor()

    # deletes all rows but keeps schema
    # cursor.execute("DELETE FROM NBA_API_GAME_IDS_DUPE")
    # conn.commit()
    # exit()
    
    update_nba_api_game_ids(conn)