import sqlite3
import pandas as pd
import sys


from basketball_stats_bot.config import load_config

def update_old_from_new(old_con, new_con):

    cursor = old_con.cursor()
    cursor.execute(f"ATTACH DATABASE '{config.LAPTOP_DB_PATH}' AS src")

    # defense vs position
    cursor.execute("DELETE FROM main.DEFENSE_VS_POSITION_2025_2026")
    cursor.execute(f"""

        INSERT INTO main.DEFENSE_VS_POSITION_2025_2026
        SELECT *
        FROM src.DEFENSE_VS_POSITION_2025_2026

    """)
    

    # dnps
    cursor.execute(f"""

        INSERT INTO main.DNPS
        SELECT *
        FROM src.DNPS
        WHERE GAME_DATE > (
            SELECT MAX(GAME_DATE) FROM main.DNPS
        )
    
    """)
    
    # minutes projection table
    cursor.execute(f"""

        INSERT INTO main.MINUTES_PROJECTION_TRAINING
        SELECT *
        FROM src.MINUTES_PROJECTION_TRAINING
        WHERE GAME_DATE > (
            SELECT MAX(GAME_DATE) FROM main.MINUTES_PROJECTION_TRAINING
        )
    
    """)

    # nba api game ids
    cursor.execute(f"""

        INSERT INTO main.NBA_API_GAME_IDS
        SELECT *
        FROM src.NBA_API_GAME_IDS
        WHERE DATE > (
            SELECT MAX(DATE) FROM main.NBA_API_GAME_IDS
        )
    
    """)

    # odds-api
    cursor.execute(f"""

        INSERT INTO main.ODDS_API
        SELECT *
        FROM src.ODDS_API
        WHERE DATE > (
            SELECT MAX(DATE) FROM main.ODDS_API
        )
    
    """)

    # player props
    cursor.execute(f"""

        INSERT INTO main.PLAYER_PROPS
        SELECT *
        FROM src.PLAYER_PROPS
        WHERE DATE > (
            SELECT MAX(DATE) FROM main.PLAYER_PROPS
        )
    
    """)

    # player vs team or last 20 jsons
    cursor.execute(f"""

        INSERT INTO main.PLAYER_VS_TEAM_OR_LAST_20_JSONS
        SELECT *
        FROM src.PLAYER_VS_TEAM_OR_LAST_20_JSONS
        WHERE DATE > (
            SELECT MAX(DATE) FROM main.PLAYER_VS_TEAM_OR_LAST_20_JSONS
        )
    
    """)

    # props training table
    cursor.execute(f"""

        INSERT INTO main.PROPS_TRAINING_TABLE
        SELECT *
        FROM src.PROPS_TRAINING_TABLE
        WHERE GAME_DATE > (
            SELECT MAX(GAME_DATE) FROM main.PROPS_TRAINING_TABLE
        )
    
    """)

    # scoreboard to roster
    cursor.execute(f"""

        INSERT INTO main.SCOREBOARD_TO_ROSTER
        SELECT *
        FROM src.SCOREBOARD_TO_ROSTER
        WHERE date > (
            SELECT MAX(date) FROM main.SCOREBOARD_TO_ROSTER
        )
    
    """)

    # system
    cursor.execute(f"""

        INSERT INTO main.SYSTEM
        SELECT *
        FROM src.SYSTEM
        WHERE DATE > (
            SELECT MAX(DATE) FROM main.SYSTEM
        )
    
    """)

    # team stats 2025-2026
    cursor.execute(f"""

        INSERT INTO main.TEAM_STATS_2025_2026
        SELECT *
        FROM src.TEAM_STATS_2025_2026
        WHERE GAME_DATE > (
            SELECT MAX(GAME_DATE) FROM main.TEAM_STATS_2025_2026
        )
    
    """)

    # team totals per player
    cursor.execute(f"""

        INSERT INTO main.TEAM_TOTALS_PER_PLAYER
        SELECT *
        FROM src.TEAM_TOTALS_PER_PLAYER
        WHERE GAME_DATE > (
            SELECT MAX(GAME_DATE) FROM main.TEAM_TOTALS_PER_PLAYER
        )
    
    """)

    # player game logs
    cursor.execute(f"""

        INSERT INTO main.player_game_logs
        SELECT *
        FROM src.player_game_logs
        WHERE GAME_DATE > (
            SELECT MAX(GAME_DATE) FROM main.player_game_logs
        )
    
    """)

    old_con.commit()

    cursor.execute("DETACH DATABASE src")

if __name__ == "__main__":

    config = load_config()

    pc_conn = sqlite3.connect(config.DB_PATH)
    lap_conn = sqlite3.connect(config.LAPTOP_DB_PATH)

    update_old_from_new(pc_conn, lap_conn)




