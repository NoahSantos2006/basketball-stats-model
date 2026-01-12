from dataclasses import dataclass
from pathlib import Path
import os
from typing import List

from dotenv import load_dotenv

# allows for os.environ['NBA_API_KEY']
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

@dataclass(frozen=True)
class Config:

    APP_NAME: str = "basketball_stats_bot"
    SEASON_START_DATE: str = "2025-10-21"
    TIMEZONE: str = "America/New_York"
    NBA_API_TIMEOUT: int = 30
    DB_PATH: Path = BASE_DIR / "basketball_stats_bot" / "data" / "data.db"
    LAPTOP_DB_PATH: Path = BASE_DIR / "basketball_stats_bot" / "data" / "data_from_laptop.db"
    PC_DB_PATH: Path = BASE_DIR / "basketball_stats_bot" / "data" / "data_from_pc.db"
    GAME_FILES_PATH: Path =  BASE_DIR / "basketball_stats_bot" /  "data" / "game_files"
    LOG_REG_PATH: Path = BASE_DIR / "basketball_stats_bot" /  "data" / "logistic_regression"
    XGBOOST_PATH: Path = BASE_DIR / "basketball_stats_bot" /  "data" / "XGBoost"
    TESTING_RESULTS_PATH: Path = BASE_DIR / "basketball_stats_bot" / "tests" / "testing_results" / "text_files"
    TESTING_RESULTS_DF_PATH: Path = BASE_DIR / "basketball_stats_bot" / "tests" / "testing_results" / "dataframes"
    API_KEY: str = ""

    CORRUPTED_GAME_ROTATION_GAME_IDS: List[int] = (
        "0022500314",
        "0022500498",
        "0022500523"
    )

def load_config() -> Config:

    return Config(

        TIMEZONE=os.getenv("TIMEZONE", "America/New_York"),
        NBA_API_TIMEOUT=int(os.getenv("NBA_API_TIMEOUT", 30)),
        API_KEY=os.environ["NBA_API_KEY"]
        
    )