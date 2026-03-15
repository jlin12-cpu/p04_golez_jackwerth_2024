"""
Pull SPX options data from OptionMetrics + zero coupon curve.

Optimizations vs original:
1. SQL filters to month-end dates only  → ~20x fewer rows per year
2. Resume / cache: already-pulled years are loaded from temp parquet,
   so a crashed run can be restarted without re-pulling completed years.

Outputs:
    _data/optionmetrics_spx_raw.parquet
    _data/optionmetrics_spx_monthly.parquet
    _data/optionmetrics_zero_curve.parquet
"""

from pathlib import Path

import pandas as pd
import wrds
from decouple import config

# =========================
# Config / Paths
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "_data"
DATA_DIR.mkdir(exist_ok=True)

WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = pd.to_datetime(config("START_DATE", default="1996-01-01"))
END_DATE = pd.to_datetime(config("END_DATE", default="2024-12-31"))

SPX_SECID = 108105
OPTION_SPX_FILE = DATA_DIR / "optionmetrics_spx_raw.parquet"
OPTION_SPX_MONTHLY_FILE = DATA_DIR / "optionmetrics_spx_monthly.parquet"
ZERO_COUPON_FILE = DATA_DIR / "optionmetrics_zero_curve.parquet"


# =========================
# Pull SPX options for a single year (month-end dates only)
# =========================

def pull_spx_by_year(year, db):
    """
    Pull SPX options for one year, restricted to month-end trading days.

    The inner subquery finds the last trading date in each calendar month,
    so we pull roughly 12 dates per year instead of ~250.
    """
    table = f"opprcd{year}"
    query = f"""
        SELECT
            date,
            exdate,
            cp_flag,
            strike_price / 1000.0  AS strike,
            best_bid,
            best_offer,
            impl_volatility,
            delta,
            gamma,
            vega,
            volume,
            open_interest,
            (best_bid + best_offer) / 2.0 AS mid_price
        FROM optionm.{table}
        WHERE secid = {SPX_SECID}
          AND date >= '{year}-01-01'
          AND date <= '{year}-12-31'
          AND cp_flag IN ('C', 'P')
          AND (best_bid + best_offer) / 2.0 >= 3
          AND (exdate - date) >= 90
          AND best_bid  > 0
          AND best_offer > 0
          -- month-end filter: keep only the last trading day of each month
          AND date IN (
              SELECT MAX(date)
              FROM optionm.{table}
              WHERE secid = {SPX_SECID}
                AND date >= '{year}-01-01'
                AND date <= '{year}-12-31'
              GROUP BY DATE_TRUNC('month', date)
          )
        ORDER BY date, exdate, strike_price, cp_flag
    """
    try:
        df = db.raw_sql(query, date_cols=["date", "exdate"])
        if not df.empty:
            df["days_to_maturity"] = (df["exdate"] - df["date"]).dt.days
            df["year"] = year
            print(f"  {year}: {len(df):>10,} rows  "
                  f"({df['date'].nunique()} month-end dates)")
        else:
            print(f"  {year}: no data")
        return df
    except Exception as e:
        print(f"  {year}: ERROR - {e}")
        return pd.DataFrame()


# =========================
# Pull full SPX options with resume support
# =========================

def pull_spx_full(start_date=START_DATE, end_date=END_DATE):
    """
    Pull SPX options year by year with two optimizations:
      - Only month-end dates are fetched from WRDS (SQL-level filter)
      - Already-completed years are loaded from temp parquet files,
        so a crashed run resumes from where it left off
    """
    start_year = start_date.year
    end_year = end_date.year

    print("=" * 60)
    print(f"Pulling SPX options {start_year} to {end_year}")
    print("(month-end dates only)")
    print("=" * 60)

    # Open one persistent connection for all years
    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    all_dfs = []

    try:
        for year in range(start_year, end_year + 1):
            temp_path = DATA_DIR / f"temp_spx_{year}.parquet"

            if temp_path.exists():
                # Resume: load from cache, skip WRDS query
                df_year = pd.read_parquet(temp_path)
                print(f"  {year}: loaded from cache ({len(df_year):,} rows)")
            else:
                df_year = pull_spx_by_year(year, db)
                if not df_year.empty:
                    df_year.to_parquet(temp_path)

            if not df_year.empty:
                all_dfs.append(df_year)

    finally:
        # Always close the connection, even if an error occurs mid-loop
        db.close()

    if not all_dfs:
        print("No data pulled.")
        return pd.DataFrame()

    df_combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\nCombined: {len(df_combined):,} rows | "
          f"{df_combined['date'].nunique()} unique dates")

    df_combined.to_parquet(OPTION_SPX_FILE)
    print(f"Saved raw SPX options to {OPTION_SPX_FILE}")

    # Clean up temp files only after successful save
    for year in range(start_year, end_year + 1):
        temp_path = DATA_DIR / f"temp_spx_{year}.parquet"
        if temp_path.exists():
            temp_path.unlink()

    return df_combined


# =========================
# Filter to month-end (already done in SQL, kept for safety)
# =========================

def filter_month_end_options(df):
    """
    Confirm / re-filter to last trading day of each month.
    Since the SQL already does this, this is essentially a no-op,
    but it guarantees correctness if the raw file is loaded from disk.
    """
    df = df.copy()
    df["year_month"] = df["date"].dt.to_period("M")
    last_days = (
        df.groupby("year_month")["date"]
        .max()
        .reset_index()
        .rename(columns={"date": "last_date"})
    )
    df = df.merge(last_days, on="year_month")
    df_month_end = df[df["date"] == df["last_date"]].copy()
    df_month_end.drop(columns=["year_month", "last_date"], inplace=True)

    print(f"Month-end filter: {len(df_month_end):,} rows | "
          f"{df_month_end['date'].nunique()} months")
    return df_month_end.reset_index(drop=True)


# =========================
# Pull zero-coupon yield curve
# =========================

def pull_zero_coupon(start_date=START_DATE, end_date=END_DATE):
    """
    Pull OptionMetrics zero-coupon yield curve (optionm.zerocd).
    """
    print("\nPulling zero coupon curve...")

    query = f"""
        SELECT date, days, rate
        FROM optionm.zerocd
        WHERE date >= '{start_date.date()}'
          AND date <= '{end_date.date()}'
        ORDER BY date, days
    """

    with wrds.Connection(wrds_username=WRDS_USERNAME) as db:
        df_zero = db.raw_sql(query, date_cols=["date"])

    print(f"Zero curve: {len(df_zero):,} rows | "
          f"{df_zero['date'].nunique()} unique dates")

    df_zero.to_parquet(ZERO_COUPON_FILE, index=False)
    print(f"Saved zero curve to {ZERO_COUPON_FILE}")

    return df_zero


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    # Pull SPX options (month-end only, with resume support)
    df_spx = pull_spx_full()

    # Re-confirm month-end filter and save monthly file
    df_month_end = filter_month_end_options(df_spx)
    df_month_end.to_parquet(OPTION_SPX_MONTHLY_FILE)
    print(f"Saved month-end SPX options to {OPTION_SPX_MONTHLY_FILE}")

    # Pull zero coupon curve
    pull_zero_coupon()

    print("\nDone.")
