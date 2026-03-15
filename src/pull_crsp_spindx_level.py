"""
Pull S&P 500 index data from CRSP.

Needed for computing strip prices and market returns in Golez & Jackwerth (2024).
Outputs: _data/crsp_sp500_daily.parquet

Columns:
- spindx  : S&P 500 price index level (for strip price calculation)
- sprtrn  : S&P 500 total return (for market returns in Table 1)
- vwretd  : CRSP value-weighted return incl. dividends (robustness)
- vwretx  : CRSP value-weighted return excl. dividends (robustness)
"""

from pathlib import Path
import pandas as pd
import wrds

from settings import config

DATA_DIR = Path(config("DATA_DIR", default="_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE", default="1996-01-01")
END_DATE = config("END_DATE", default="2025-12-31")


def pull_spx_daily(
    start_date=START_DATE,
    end_date=END_DATE,
    wrds_username=WRDS_USERNAME
):
    """
    Pull daily S&P 500 data from CRSP dsi.

    spindx : index level for put-call parity strip price calculation
    sprtrn : S&P 500 return for market return series (Table 1)
    vwretd : value-weighted return incl. dividends (alternative market return)
    vwretx : value-weighted return excl. dividends (to back out realized dividends)
    """
    query = f"""
    SELECT
        date,
        spindx,
        sprtrn,
        vwretd,
        vwretx
    FROM crsp.dsi
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query, date_cols=["date"])
    db.close()
    return df


if __name__ == "__main__":
    df = pull_spx_daily()
    out_path = DATA_DIR / "crsp_sp500_daily.parquet"
    df.to_parquet(out_path, index=False)

    print(f"✓ Saved to {out_path}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Rows: {len(df):,}")
    print(f"  Missing values:\n{df.isna().sum()}")
    print(df.head())