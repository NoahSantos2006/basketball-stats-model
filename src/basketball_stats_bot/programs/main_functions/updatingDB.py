import sqlite3
import time
from datetime import date
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from basketball_stats_bot.config import load_config

from basketball_stats_bot.programs.updating_functions.model_training import (
    train_minutes_projection_model, 
    train_v7_model,
    train_v8_model,
    train_v9_model
)
from basketball_stats_bot.programs.updating_functions.players import (
    update_db_gamelogs,
    update_dnps_table,
    update_scoreboard_to_team_roster,
    update_dnps_from_nbainjuries,
    update_team_totals_per_player,
)
from basketball_stats_bot.programs.updating_functions.team_updating import (
    update_defense_vs_position,
    update_team_stats
)
from basketball_stats_bot.programs.updating_functions.training_tables import (
    update_minutes_projection_features_table,
    update_props_training_table
)
from basketball_stats_bot.programs.updating_functions.game_ids import (
    update_nba_api_game_ids,
    update_odds_api_game_ids
)
from basketball_stats_bot.programs.updating_functions.grading import update_system

def updateDB(API_KEY, curr_date, current_season_start_date, conn, current_season):

    update_system(conn=conn)
    update_db_gamelogs(conn=conn)
    update_scoreboard_to_team_roster(conn=conn, current_season=current_season)
    update_team_totals_per_player(conn=conn)
    update_odds_api_game_ids(conn=conn, api_key=API_KEY)
    update_nba_api_game_ids(conn=conn)
    update_defense_vs_position(conn=conn, current_season_start_date=current_season_start_date)
    update_team_stats(conn=conn)
    update_dnps_table(conn=conn, season_start_date=current_season_start_date)
    update_dnps_from_nbainjuries(conn=conn, season_start_date=current_season_start_date, curr_date=curr_date)

    update_minutes_projection_features_table(conn=conn, season_start_date=current_season_start_date)
    update_props_training_table(season_start_date=current_season_start_date, conn=conn)

    train_minutes_projection_model(conn=conn)
    # train_v7_model(conn=conn)
    # train_v8_model(conn=conn)
    train_v9_model(conn=conn)

    print(f"SQL database updated. ({curr_date})")

if __name__ == "__main__":

    config = load_config()
    curr_date = datetime.now(ZoneInfo(config.TIMEZONE)).date()
    current_season_start_date = "2025-10-21"
    current_season = "2025-26"

    conn = sqlite3.connect(config.DB_PATH)

    updateDB(config.API_KEY, curr_date, current_season_start_date, conn, current_season)