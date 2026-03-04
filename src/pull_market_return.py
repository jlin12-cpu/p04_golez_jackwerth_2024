# ============================================================
# Pull CRSP Value-Weighted Market Return
# ============================================================

from pathlib import Path
import pandas as pd
import wrds
from decouple import config

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "_data"
DATA_DIR.mkdir(exist_ok=True)

WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = "1996-01-01"
END_DATE = "2024-12-31"

OUTPUT_FILE = DATA_DIR / "crsp_market_return.parquet"

def pull_market_return():

    print("Connecting to WRDS...")
    db = wrds.Connection(wrds_username=WRDS_USERNAME)

    query = f"""
        SELECT date, vwretd
        FROM crsp.msi
        WHERE date >= '{START_DATE}'
          AND date <= '{END_DATE}'
        ORDER BY date
    """

    df = db.raw_sql(query, date_cols=["date"])
    db.close()

    df.rename(columns={
        "vwretd": "market_ret"
    }, inplace=True)

    print(f"Pulled {len(df)} observations.")

    df.to_parquet(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    pull_market_return()
