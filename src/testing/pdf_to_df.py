import requests
import pandas as pd
import pdfplumber
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO
import tabula
from zoneinfo import ZoneInfo
import unicodedata
from datetime import datetime
import sys
from pathlib import Path
import sqlite3

from nbainjuries import injury
import nbainjuries as injury_mod

from basketball_stats_bot.config import load_config



import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import tabula
import pandas as pd
import tempfile


def download_nba_pdf(url: str, timeout=(5, 30)) -> Path:
    """
    Robustly download an NBA injury report PDF to a temp file.
    Returns local Path.
    """
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    session.mount("https://", HTTPAdapter(max_retries=retries))

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf",
        "Accept-Encoding": "identity",
        "Range": "bytes=0-"
    }

    with session.get(
        url,
        headers=headers,
        stream=True,
        timeout=timeout,
    ) as r:
        r.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        with open(tmp.name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    return Path(tmp.name)


def parse_injury_report_pdf(pdf_path: Path) -> pd.DataFrame:
    """
    Parse NBA injury report PDF into a DataFrame using tabula.
    """
    dfs = tabula.read_pdf(
        pdf_path,
        pages="all",
        stream=True,
        guess=False,
        pandas_options={"header": None, "dtype": str}
    )

    raw = pd.concat(dfs, ignore_index=True).dropna(how="all")

    return raw


if __name__ == "__main__":

    url = "https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-01-14_04_00PM.pdf"

    print("Downloading PDF...")
    pdf_path = download_nba_pdf(url)

    print(f"Saved to {pdf_path}")
    print("Parsing PDF...")

    injury_df = parse_injury_report_pdf(pdf_path)

    players = injury_df[3].to_list()

    out = []

    for player in players:

        if pd.isna(player):
            continue
        curr = player.split(',')

        if len(curr) > 1:

            out.append(curr)
    
    print(out)


