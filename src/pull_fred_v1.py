"""
Pull Treasury rates from FRED for dividend strip analysis.

For Golez & Jackwerth (2024):
- 1-month T-bill rate (risk-free rate) 
- 2-year Treasury rate
- 10-year Treasury rate
"""

from pathlib import Path
import pandas as pd
import pandas_datareader.data as web

from settings import config

DATA_DIR = Path(config("DATA_DIR", default="_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
START_DATE = config("START_DATE", default="1996-01-01")
END_DATE = config("END_DATE", default="2025-12-31")


def pull_treasury_rates(start_date=START_DATE, end_date=END_DATE):
    """
    Pull Treasury rates from FRED.
    
    Series codes:
    - DGS1MO: 1-Month Treasury Constant Maturity Rate
    - DGS2: 2-Year Treasury Constant Maturity Rate  
    - DGS10: 10-Year Treasury Constant Maturity Rate
    
    Returns:
    --------
    DataFrame with daily Treasury rates (as decimals, not percentages)
    """
    
    series_codes = ['DGS2', 'DGS10']
    
    print(f"Pulling Treasury rates from FRED...")
    print(f"  Period: {start_date} to {end_date}")
    
    # Pull data
    df = web.DataReader(series_codes, 'fred', start_date, end_date)
    
    # Rename columns to be more descriptive
    df = df.rename(columns={
        'DGS2': 'treasury_2y', 
        'DGS10': 'treasury_10y'
    })
    
    # Convert from percentage to decimal (FRED gives percentages)
    df = df / 100
    
    # Reset index to make date a column
    df = df.reset_index().rename(columns={'DATE': 'date'})
    
    print(f"Pulled {len(df)} days of data")
    print(f"Missing values:")
    print(f"treasury_2y: {df['treasury_2y'].isna().sum()}")
    print(f"treasury_10y: {df['treasury_10y'].isna().sum()}")
    
    return df


def load_treasury_rates(data_dir=DATA_DIR):
    """
    Load previously saved Treasury rates.
    """
    file_path = Path(data_dir) / "fred_treasury_rates.parquet"
    df = pd.read_parquet(file_path)
    return df


if __name__ == "__main__":
    # Pull data
    df = pull_treasury_rates()
    
    # Save
    filedir = Path(DATA_DIR)
    filedir.mkdir(parents=True, exist_ok=True)
    
    output_path = filedir / "fred_treasury_rates.parquet"
    df.to_parquet(output_path)
    
    print(f"\n Saved to {output_path}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Shape: {df.shape}")
    print(f"\nFirst few rows:")
    print(df.head())
    print(f"\nLast few rows:")
    print(df.tail())