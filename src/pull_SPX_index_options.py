from pathlib import Path
import pandas as pd
import wrds
from decouple import config

# =========================
# Paths & config
# =========================
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "_data"
DATA_DIR.mkdir(exist_ok=True)

WRDS_USERNAME = config("WRDS_USERNAME")
START_YEAR = int(config("START_YEAR", default=1996))
END_YEAR = int(config("END_YEAR", default=2024))

OUTPUT_FILE = DATA_DIR / "optionm_spx_options.parquet"

SPX_SECID = 108105  

# =========================
# WRDS connection
# =========================
def connect_wrds():
    print("\nConnecting to WRDS...")
    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    print("Connected.")
    return db

# =========================
# Pull one year
# =========================
def pull_spx_options_by_year(db, year):
    table = f"opprcd{year}"

    query = f"""
        SELECT
            date,
            exdate,
            cp_flag,
            strike_price / 1000 AS strike,
            best_bid,
            best_offer,
            volume,
            open_interest,
            impl_volatility,
            (best_bid + best_offer) / 2 AS mid_price
        FROM optionm.{table}
        WHERE
            secid = {SPX_SECID}
            AND cp_flag IN ('C', 'P')
            AND best_bid > 0
            AND best_offer > 0
            AND (best_bid + best_offer) / 2 >= 3
            AND (exdate - date) >= 90
        ORDER BY date, exdate, strike_price, cp_flag
    """

    print(f"Pulling {year}...")
    try:
        df = db.raw_sql(query, date_cols=["date", "exdate"])
        if df.empty:
            print(f"  No data for {year}")
            return pd.DataFrame()

        df["year"] = year
        df["days_to_maturity"] = (df["exdate"] - df["date"]).dt.days

        print(f"  Got {len(df):,} rows")
        return df

    except Exception as e:
        print(f"  Skipped {year}: {e}")
        return pd.DataFrame()

# =========================
# Pull all years
# =========================
def pull_spx_index_options(start_year=START_YEAR, end_year=END_YEAR):
    db = connect_wrds()
    all_dfs = []

    for year in range(start_year, end_year + 1):
        df_year = pull_spx_options_by_year(db, year)
        if not df_year.empty:
            all_dfs.append(df_year)

    db.close()

    if not all_dfs:
        raise RuntimeError("No SPX option data pulled.")

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal rows: {len(df):,}")
    return df

# =========================
# Main
# =========================
if __name__ == "__main__":
    df = pull_spx_index_options()
    print("\nSaving to:", OUTPUT_FILE)
    df.to_parquet(OUTPUT_FILE, index=False)
    print("Done.")
