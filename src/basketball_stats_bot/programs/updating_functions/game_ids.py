from datetime import datetime, timedelta
from nba_api.live.nba.endpoints import scoreboard
from zoneinfo import ZoneInfo
import requests
from nba_api.stats.endpoints import leaguegamefinder
import sqlite3
import pandas as pd
import sys

from basketball_stats_bot.config import load_config

def update_nba_api_game_ids(conn):

    config = load_config()

    def find_game_ids_by_date(cursor, latest_date_str):

        curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
        today = datetime.now(ZoneInfo(config.TIMEZONE)).date()

        while curr_date <= today:

            cursor.execute(f"""

                SELECT * FROM NBA_API_GAME_IDS
                WHERE DATE = ?

            """, (str(curr_date),))

            fetch = cursor.fetchall()

            if fetch:

                print(f"Already found NBA API game ids for {str(curr_date)}")
                curr_date += timedelta(days=1)
                continue

            print(f"Finding NBA API game ids for {str(curr_date)}...")


            if curr_date == today:

                board = scoreboard.ScoreBoard()
                games = board.games.get_dict()

                yesterday = curr_date - timedelta(days=1)
                yesterday_code = "".join(str(yesterday).split("-"))

                for game in games:
                    
                    if game['gameCode'][:8] == yesterday_code:

                        continue

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

    find_game_ids_by_date(cursor, latest_date)

    conn.commit()
    
    print("NBA_API game ids updated")

def update_odds_api_game_ids(conn, api_key):

    config = load_config()

    def retrieve_odds_api_game_ids(cursor, latest_date_str, API_KEY):

        curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()

        today = datetime.now(ZoneInfo("America/New_York")).date()

        while curr_date <= today:

            check_for_existing = pd.read_sql_query("SELECT * FROM ODDS_API WHERE DATE = ?", conn, params=(str(curr_date),))
                                                   
            if not check_for_existing.empty:

                curr_date += timedelta(days=1)
                continue

            commenceTimeFrom = f"{curr_date}T09:00:00Z"
            day_after = curr_date + timedelta(days=1)
            commenceTimeTo = f"{day_after}T04:59:59Z"

            params = {
                "apiKey": API_KEY,
                "date": commenceTimeFrom,
                "commenceTimeTo": commenceTimeTo
            }

            today = datetime.now(ZoneInfo(config.TIMEZONE)).date()
            today_str = datetime.strftime(today, "%Y-%m-%d")

            if curr_date == today:

                url = "https://api.the-odds-api.com/v4/sports/basketball_nba/events"
            
            else:

                url = "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events"

            response = requests.get(url, params=params)

            data = response.json()

            print(f"Retrieving Odds-api game ids for {curr_date}...")

            if curr_date == today:

                for val in data:

                    cursor.execute("""

                    INSERT OR REPLACE INTO ODDS_API (DATE, GAME_ID)
                    VALUES (?, ?)
                    
                """, (str(curr_date), val['id']))
            
            else:

                for val in data['data']:
                    
                    cursor.execute("""

                        INSERT OR REPLACE INTO ODDS_API (DATE, GAME_ID)
                        VALUES (?, ?)
                        
                    """, (str(curr_date), val['id']))
            
            
            curr_date += timedelta(days=1)

    cursor = conn.cursor()

    cursor.execute("""

        SELECT *
        FROM ODDS_API
        ORDER BY DATE DESC
        LIMIT 1

    """)

    fetch = cursor.fetchall()

    latest_date_str = fetch[0][0]

    retrieve_odds_api_game_ids(cursor, latest_date_str, api_key)

    conn.commit()

    print("Odds-api game ids updated.")

if __name__ == "__main__":

    config = load_config()

    conn = sqlite3.connect(config.DB_ONE_DRIVE_PATH)

    update_nba_api_game_ids(conn=conn)