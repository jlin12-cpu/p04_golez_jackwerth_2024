"""
Pull SPX options data from OptionMetrics.

For Golez & Jackwerth (2024):
- European SPX options (secid = 108105)
- Last business day of each month
- Maturities >= 90 days
- Bid-ask midpoint >= $3
- Will filter moneyness 0.5-1.5 later in analysis

Paper methodology (page 11-12):
"We obtain data on European S&P 500 index options (henceforth SPX options)
from the Chicago Board of Options Exchange (CBOE). We use tick-level data
for the period from January 1, 1990, through March 31, 2004, and minute-level
data from January 1, 2004, through December 31, 2022."

Note: We use OptionMetrics instead of raw CBOE data, which provides
cleaned and standardized options data.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import wrds
from datetime import datetime

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE", default="1996-01-01")
END_DATE = config("END_DATE", default="2025-12-31")


def pull_spx_options_by_year(
    year,
    wrds_username=WRDS_USERNAME
):
    """
    Pull SPX options data for a single year.
    
    This function pulls data for one year at a time to manage memory.
    
    Parameters:
    -----------
    year : int
        Year to pull data for (e.g., 2020)
    
    Returns:
    --------
    DataFrame with SPX options data
    """
    
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # OptionMetrics uses different table names by year
    # Before 1996: opprcd1996
    # 1996-2022: opprcd{year}
    # We need to check which table to use
    
    query = f"""
    SELECT 
        date,
        exdate,
        cp_flag,
        strike_price / 1000 as strike,
        best_bid,
        best_offer,
        impl_volatility,
        delta,
        gamma,
        vega,
        volume,
        open_interest,
        (best_bid + best_offer) / 2 as mid_price
    FROM optionm.opprcd{year}
    WHERE 
        secid = 108105
        AND date >= '{start_date}'
        AND date <= '{end_date}'
        AND cp_flag IN ('C', 'P')
        AND (best_bid + best_offer) / 2 >= 3
        AND (exdate - date) >= 90
        AND best_bid > 0
        AND best_offer > 0
    ORDER BY date, exdate, strike_price, cp_flag
    """
    
    print(f"  Pulling data for {year}...")
    
    try:
        db = wrds.Connection(wrds_username=wrds_username)
        df = db.raw_sql(query, date_cols=["date", "exdate"])
        db.close()
        
        # Calculate additional fields
        df['days_to_maturity'] = (df['exdate'] - df['date']).dt.days
        df['year'] = year
        
        print(f" Got {len(df):,} observations for {year}")
        
        return df
        
    except Exception as e:
        print(f" Error for {year}: {e}")
        return pd.DataFrame()


def pull_spx_options_full(
    start_date=START_DATE,
    end_date=END_DATE,
    wrds_username=WRDS_USERNAME
):
    """
    Pull full SPX options dataset by iterating through years.
    
    This approach:
    1. Pulls data year by year to manage memory
    2. Saves intermediate results
    3. Combines all years at the end
    """
    
    # Parse dates
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    
    print(f"Pulling SPX options data from {start_year} to {end_year}")
    print(f"This may take several minutes...\n")
    
    all_dfs = []
    
    for year in range(start_year, end_year + 1):
        df_year = pull_spx_options_by_year(year, wrds_username)
        
        if not df_year.empty:
            all_dfs.append(df_year)
            
            # Save intermediate result
            temp_path = Path(DATA_DIR) / f"temp_options_{year}.parquet"
            df_year.to_parquet(temp_path)
    
    # Combine all years
    print(f"\nCombining all years...")
    df_combined = pd.concat(all_dfs, ignore_index=True)
    
    print(f" Total observations: {len(df_combined):,}")
    print(f"  Date range: {df_combined['date'].min()} to {df_combined['date'].max()}")
    print(f"  Unique dates: {df_combined['date'].nunique()}")
    
    # Clean up temporary files
    print(f"\nCleaning up temporary files...")
    for year in range(start_year, end_year + 1):
        temp_path = Path(DATA_DIR) / f"temp_options_{year}.parquet"
        if temp_path.exists():
            temp_path.unlink()
    
    return df_combined


def filter_month_end_options(df):
    """
    Filter to keep only options from last business day of each month.
    
    Following the paper methodology.
    """
    
    # Get last business day of each month
    df['year_month'] = df['date'].dt.to_period('M')
    
    # For each month, find the last trading day
    last_days = df.groupby('year_month')['date'].max().reset_index()
    last_days.columns = ['year_month', 'last_date']
    
    # Merge and filter
    df = df.merge(last_days, on='year_month')
    df_filtered = df[df['date'] == df['last_date']].copy()
    df_filtered = df_filtered.drop(columns=['year_month', 'last_date'])
    
    print(f"After filtering to month-end: {len(df_filtered):,} observations")
    print(f"  Unique months: {df_filtered['date'].dt.to_period('M').nunique()}")
    
    return df_filtered


def load_spx_options(data_dir=DATA_DIR):
    """
    Load previously saved SPX options data.
    """
    file_path = Path(data_dir) / "optionmetrics_spx_raw.parquet"
    df = pd.read_parquet(file_path)
    return df


if __name__ == "__main__":
    print("="*70)
    print("PULLING SPX OPTIONS DATA FROM OPTIONMETRICS")
    print("="*70)
    print()
    
    # Pull all data
    df = pull_spx_options_full(
        start_date=START_DATE,
        end_date=END_DATE,
        wrds_username=WRDS_USERNAME
    )
    
    # Save raw data
    output_path_raw = Path(DATA_DIR) / "optionmetrics_spx_raw.parquet"
    df.to_parquet(output_path_raw)
    print(f"\n✓ Saved raw data to {output_path_raw}")
    
    # Filter to month-end only
    print("\n" + "="*70)
    print("FILTERING TO MONTH-END OBSERVATIONS")
    print("="*70)
    print()
    
    df_month_end = filter_month_end_options(df)
    
    # Save month-end data
    output_path_monthly = Path(DATA_DIR) / "optionmetrics_spx_monthly.parquet"
    df_month_end.to_parquet(output_path_monthly)
    print(f"\n Saved month-end data to {output_path_monthly}")
    
    # Summary statistics
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)
    print(f"\nRaw data:")
    print(f"  Shape: {df.shape}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
    
    print(f"\nMonth-end data:")
    print(f"  Shape: {df_month_end.shape}")
    print(f"  Unique months: {df_month_end['date'].dt.to_period('M').nunique()}")
    
    print(f"\nFirst few rows of month-end data:")
    print(df_month_end.head(10))
    
    print(f"\nLast few rows of month-end data:")
    print(df_month_end.tail(10))
    
    print("\n" + "="*70)
    print("DONE!")
    print("="*70)