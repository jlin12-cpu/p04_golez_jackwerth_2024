"""
Pull Treasury bond returns from CRSP.

For Golez & Jackwerth (2024):
- 2-year Treasury returns (for strip excess returns)
- 10-year Treasury returns (for market excess returns)

Paper reference (page 12):
"We obtain returns on 2- and 10-year fixed maturity Treasuries from CRSP."
"""

from pathlib import Path
import pandas as pd
import wrds

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE", default="1996-01-01")
END_DATE = config("END_DATE", default="2025-12-31")


def pull_treasury_returns(
    start_date=START_DATE,
    end_date=END_DATE,
    wrds_username=WRDS_USERNAME
):
    """
    Pull monthly Treasury bond returns from CRSP.
    
    Uses CRSP Monthly Treasury Returns (MCTI table).
    
    Returns:
    --------
    DataFrame with columns:
        - date: Month-end date
        - treasury_2y_ret: 2-year Treasury total return
        - treasury_10y_ret: 10-year Treasury total return
    """
    
    # CRSP table: crsp.mcti (Monthly Constant Maturity Treasury Index)
    query = f"""
    SELECT 
        caldt as date,
        b2ret as treasury_2y_ret,
        b10ret as treasury_10y_ret
    FROM crsp.mcti
    WHERE caldt BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY caldt
    """
    
    print(f"Connecting to WRDS as {wrds_username}...")
    db = wrds.Connection(wrds_username=wrds_username)
    
    print(f"Pulling Treasury returns from {start_date} to {end_date}...")
    df = db.raw_sql(query, date_cols=["date"])
    db.close()
    
    print(f"Pulled {len(df)} months of data")
    
    # Check for missing values
    print(f"  Missing values:")
    print(f"    treasury_2y_ret: {df['treasury_2y_ret'].isna().sum()}")
    print(f"    treasury_10y_ret: {df['treasury_10y_ret'].isna().sum()}")
    
    return df


def load_treasury_returns(data_dir=DATA_DIR):
    """
    Load previously saved Treasury returns.
    """
    file_path = Path(data_dir) / "crsp_treasury_returns.parquet"
    df = pd.read_parquet(file_path)
    return df


if __name__ == "__main__":
    # Pull data
    df = pull_treasury_returns()
    
    # Save
    output_path = Path(DATA_DIR) / "crsp_treasury_returns.parquet"
    df.to_parquet(output_path)
    
    print(f"\n Saved to {output_path}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Shape: {df.shape}")
    print(f"\nFirst few rows:")
    print(df.head())
    print(f"\nLast few rows:")
    print(df.tail())