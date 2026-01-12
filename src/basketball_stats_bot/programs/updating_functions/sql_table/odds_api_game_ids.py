import requests
from datetime import date, timedelta, datetime
import sqlite3
import json
from zoneinfo import ZoneInfo

def update_odds_api_game_ids(conn, api_key):

    def retrieve_odds_api_game_ids(cursor, latest_date_str, API_KEY):

        url = "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events"

        curr_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()

        today = datetime.now(ZoneInfo("America/New_York")).date()

        while curr_date <= today:
            
            cursor.execute("SELECT * FROM ODDS_API WHERE DATE = ?", (str(curr_date),))
            fetch = cursor.fetchall()

            if fetch:

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

            response = requests.get(url, params=params)

            data = response.json()

            print(f"Retrieving Odds-api game ids for {curr_date}...")

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

    conn = sqlite3.connect(r"C:\Users\noahs\.vscode\basketball stats bot\main\game_data\data.db")
    API_KEY = "YOUR API KEY"

    update_odds_api_game_ids(conn, API_KEY)

