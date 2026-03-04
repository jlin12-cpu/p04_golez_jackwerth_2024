# ============================================================
# Pull SPX Spot Price from OptionMetrics
# ============================================================

from pathlib import Path
import pandas as pd
import wrds
from decouple import config

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "_data"
DATA_DIR.mkdir(exist_ok=True)

WRDS_USERNAME = config("WRDS_USERNAME")
OUTPUT_FILE = DATA_DIR / "spx_spot.parquet"

START_DATE = "1996-01-01"
END_DATE = "2024-12-31"

def pull_spx_spot():

    print("Connecting to WRDS...")
    db = wrds.Connection(wrds_username=WRDS_USERNAME)

    query = f"""
        SELECT date, close
        FROM optionm.secprd
        WHERE secid = 108105
          AND date >= '{START_DATE}'
          AND date <= '{END_DATE}'
        ORDER BY date
    """

    df = db.raw_sql(query, date_cols=["date"])
    db.close()

    df.rename(columns={"close": "spx_price"}, inplace=True)

    df.to_parquet(OUTPUT_FILE, index=False)

    print(f"Saved SPX spot price to {OUTPUT_FILE}")

if __name__ == "__main__":
    pull_spx_spot()
