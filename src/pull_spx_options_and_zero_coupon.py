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

        query = """
            SELECT date, days, rate
            FROM optionm.zerocd
            WHERE date >= '1996-01-01'
              AND date <= '2024-12-31'
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


