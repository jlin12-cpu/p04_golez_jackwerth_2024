"""
Pull S&P 500 index data from CRSP.

For replicating Golez & Jackwerth (2024) "Holding Period Effects in Dividend Strip Returns"
We need daily S&P 500 index returns to calculate realized dividends.
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


def pull_sp500_daily(
    start_date=START_DATE, 
    end_date=END_DATE, 
    wrds_username=WRDS_USERNAME
):
    """
    Pull daily S&P 500 index data from CRSP.
    
    Returns:
    --------
    DataFrame with columns:
        - date: Trading date
        - vwretd: Value-weighted return including dividends
        - vwretx: Value-weighted return excluding dividends
        - sprtrn: S&P 500 return
    """
    
    query = f"""
    SELECT 
        date,
        vwretd,
        vwretx,
        sprtrn
    FROM crsp.dsi
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    
    print(f"Connecting to WRDS as {wrds_username}...")
    db = wrds.Connection(wrds_username=wrds_username)
    
    print(f"Pulling data from {start_date} to {end_date}...")
    df = db.raw_sql(query, date_cols=["date"])
    db.close()
    
    print(f"Pulled {len(df)} days of data")
    
    return df


if __name__ == "__main__":
    # Pull the data
    df = pull_sp500_daily(start_date=START_DATE, end_date=END_DATE)
    
    # Save to _data directory
    output_path = Path(DATA_DIR) / "crsp_sp500_daily.parquet"
    df.to_parquet(output_path)
    
    print(f"\n Saved to {output_path}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Shape: {df.shape}")
    print(f"\nFirst few rows:")
    print(df.head())