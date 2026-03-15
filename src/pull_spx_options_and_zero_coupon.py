"""
Pull raw SPX options and zero-curve data from OptionMetrics via WRDS.

This script downloads two key raw data inputs used in the replication of
Golez and Jackwerth (2024), "Holding Period Effects in Dividend Strip Returns":

1. European S&P 500 index options (SPX options) from OptionMetrics IvyDB
2. Daily zero-coupon yield curve data from OptionMetrics

For the options data, the script:
- pulls data year by year to manage memory,
- filters to European SPX options (secid = 108105),
- keeps options with maturity of at least 90 days,
- requires positive bid and ask quotes,
- requires bid-ask midpoint of at least $3,
- saves the full raw options panel, and
- constructs a month-end options file by keeping only the last trading day
  of each month.

For the zero-curve data, the script downloads the daily OptionMetrics
zero-coupon term structure over the project sample period.

Outputs
-------
_data/optionmetrics_spx_raw.parquet
    Full raw SPX options panel pulled from OptionMetrics.

_data/optionmetrics_spx_monthly.parquet
    SPX options filtered to the last trading day of each month.

_data/optionmetrics_zero_curve.parquet
    Daily zero-coupon curve from OptionMetrics.

Notes
-----
This script is part of the raw-data pull layer of the project pipeline.
It is intentionally separate from the cleaning and analysis scripts.
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
END_DATE = pd.to_datetime(config("END_DATE", default="2025-12-31"))

SPX_SECID = 108105
OPTION_SPX_FILE = DATA_DIR / "optionmetrics_spx_raw.parquet"
OPTION_SPX_MONTHLY_FILE = DATA_DIR / "optionmetrics_spx_monthly.parquet"
ZERO_COUPON_FILE = DATA_DIR / "optionmetrics_zero_coupon.parquet"

# =========================
# Pull SPX options for a single year
# =========================
def pull_spx_by_year(year, db):
    table = f"opprcd{year}"
    query = f"""
        SELECT 
            date,
            exdate,
            cp_flag,
            strike_price/1000 as strike,
            best_bid,
            best_offer,
            impl_volatility,
            delta,
            gamma,
            vega,
            volume,
            open_interest,
            (best_bid + best_offer)/2 as mid_price
        FROM optionm.{table}
        WHERE secid = {SPX_SECID}
          AND date >= '{year}-01-01' AND date <= '{year}-12-31'
          AND cp_flag IN ('C','P')
          AND (best_bid + best_offer)/2 >= 3
          AND (exdate - date) >= 90
          AND best_bid > 0 AND best_offer > 0
        ORDER BY date, exdate, strike_price, cp_flag
    """
    try:
        df = db.raw_sql(query, date_cols=["date","exdate"])
        if not df.empty:
            df["days_to_maturity"] = (df["exdate"] - df["date"]).dt.days
            df["year"] = year
            print(f"  Got {len(df):,} rows for {year}")
        else:
            print(f"  No data for {year}")
        return df
    except Exception as e:
        print(f"  Error for {year}: {e}")
        return pd.DataFrame()

# =========================
# Pull full SPX options
# =========================
def pull_spx_full(start_date=START_DATE, end_date=END_DATE):
    start_year = start_date.year
    end_year = end_date.year
    print(f"Pulling SPX options {start_year} to {end_year}")

    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    all_dfs = []

    for year in range(start_year, end_year + 1):
        df_year = pull_spx_by_year(year, db)
        if not df_year.empty:
            all_dfs.append(df_year)
            # Save intermediate
            temp_path = DATA_DIR / f"temp_spx_{year}.parquet"
            df_year.to_parquet(temp_path)

    df_combined = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    
    db.close()
    print(f"Combined SPX options: {len(df_combined):,} rows")
    
    df_combined.to_parquet(OPTION_SPX_FILE)
    print(f"Saved raw SPX options to {OPTION_SPX_FILE}")

    # Clean up temp files
    for year in range(start_year, end_year + 1):
        temp_path = DATA_DIR / f"temp_spx_{year}.parquet"
        if temp_path.exists():
            temp_path.unlink()

    return df_combined

# =========================
# Pull zero-coupon zero curve
# =========================
def pull_zero_coupon():
    import wrds
    
    print("Pulling OptionMetrics zero curve...")

    with wrds.Connection(wrds_username=WRDS_USERNAME) as db:

        query = f"""
            SELECT date, days, rate
            FROM optionm.zerocd
            WHERE date >= '{START_DATE.date()}'
              AND date <= '{END_DATE.date()}'
            ORDER BY date, days
        """

        df_zero = db.raw_sql(query, date_cols=["date"])

    print(f"Got {len(df_zero):,} zero curve rows")

    df_zero.to_parquet(DATA_DIR / "optionmetrics_zero_curve.parquet", index=False)
    print("Saved zero curve to _data/optionmetrics_zero_curve.parquet")

    return df_zero


# =========================
# Filter to month-end
# =========================
def filter_month_end_options(df):
    df["year_month"] = df["date"].dt.to_period("M")
    last_days = df.groupby("year_month")["date"].max().reset_index()
    last_days.columns = ["year_month", "last_date"]
    df = df.merge(last_days, on="year_month")
    df_month_end = df[df["date"] == df["last_date"]].copy()
    df_month_end.drop(columns=["year_month","last_date"], inplace=True)
    df_month_end.to_parquet(OPTION_SPX_MONTHLY_FILE)
    print(f"Saved month-end SPX options to {OPTION_SPX_MONTHLY_FILE}")
    return df_month_end

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print("="*70)
    print("PULLING SPX OPTIONS AND ZERO COUPON")
    print("="*70)
    
    df_spx = pull_spx_full()
    df_month_end = filter_month_end_options(df_spx)
    df_zero = pull_zero_coupon()
    
    print("\nDone pulling and saving all data!")


