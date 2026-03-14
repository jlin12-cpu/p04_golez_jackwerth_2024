"""
Pull Treasury rates and Fama-French factors.

Outputs three files:
1. fred_treasury_rates.parquet - Treasury and short-rate data
   - 1-month T-bill rate proxy (risk-free rate from Kenneth French data)
   - 1-year Treasury constant maturity rate
   - 2-year Treasury constant maturity rate
   - 10-year Treasury constant maturity rate
2. fama_french_factors.parquet - All Fama-French factors including market factor
3. fama_french_monthly.parquet
"""

from pathlib import Path
import pandas as pd
import pandas_datareader.data as web

from settings import config

DATA_DIR = Path(config("DATA_DIR", default="_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
START_DATE = config("START_DATE", default="1996-01-01")
END_DATE = config("END_DATE", default="2025-12-31")


def pull_fama_french_full(start_date=START_DATE, end_date=END_DATE):
    """
    Pull complete Fama-French factors including market factor.
    
    Returns:
    --------
    DataFrame with columns: date, mkt_rf, smb, hml, rf_1m
    """
    
    print(f"Pulling Fama-French factors (including market factor)...")
    
    # Pull Fama-French 3 factors
    ff_data = web.DataReader('F-F_Research_Data_Factors_daily', 
                             'famafrench', 
                             start_date, 
                             end_date)
    
    # Get daily data
    df = ff_data[0]
    
    # Convert from percentage to decimal
    df = df / 100
    
    # Reset index
    df = df.reset_index().rename(columns={'Date': 'date'})
    
    # Rename columns
    df = df.rename(columns={
        'Mkt-RF': 'mkt_rf',
        'SMB': 'smb',
        'HML': 'hml',
        'RF': 'rf_1m'
    })
    
    print(f"Got {len(df)} days of Fama-French data")
    
    return df


def pull_fred_rates(start_date=START_DATE, end_date=END_DATE):
    """
    Pull 1-year, 2-year, and 10-year Treasury constant maturity rates from FRED.
    """
    
    print(f"Pulling 1-year, 2-year, and 10-year Treasury rates from FRED...")
    
    series_codes = ['DGS1', 'DGS2', 'DGS10']
    
    df = web.DataReader(series_codes, 'fred', start_date, end_date)
    
    df = df.rename(columns={
        'DGS1': 'treasury_1y',
        'DGS2': 'treasury_2y', 
        'DGS10': 'treasury_10y'
    })
    
    df = df / 100
    df = df.reset_index().rename(columns={'DATE': 'date'})
    
    print(f"Got {len(df)} days of FRED data")
    
    return df


def pull_all_data(start_date=START_DATE, end_date=END_DATE):
    """
    Pull all data and create three output files.
    """
    
    print(f"=" * 70)
    print(f"PULLING TREASURY RATES AND FAMA-FRENCH FACTORS")
    print(f"=" * 70)
    print(f"Period: {start_date} to {end_date}\n")
    
    # Pull from both sources
    df_ff = pull_fama_french_full(start_date, end_date)
    df_fred = pull_fred_rates(start_date, end_date)
    
    # Create treasury rates file
    print(f"\nCreating treasury rates file")
    df_treasury = pd.merge(
        df_ff[['date', 'rf_1m']], 
        df_fred, 
        on='date', 
        how='outer'
    )
    df_treasury = df_treasury.sort_values('date').reset_index(drop=True)
    df_treasury = df_treasury[['date', 'rf_1m', 'treasury_1y', 'treasury_2y', 'treasury_10y']]
    
    print(f"Treasury rates: {df_treasury.shape}")
    print(f"Missing values:")
    print(f"rf_1m:        {df_treasury['rf_1m'].isna().sum()}")
    print(f"treasury_1y:  {df_treasury['treasury_1y'].isna().sum()}")
    print(f"treasury_2y:  {df_treasury['treasury_2y'].isna().sum()}")
    print(f"treasury_10y: {df_treasury['treasury_10y'].isna().sum()}")
    
    # Fama-French factors file 
    print(f"\nFama-French factors: {df_ff.shape}")
    print(f"  Columns: {df_ff.columns.tolist()}")
    
    return df_treasury, df_ff

def pull_fama_french_monthly(start_date=START_DATE, end_date=END_DATE):
    """
    Pull monthly Fama-French factors from Kenneth French's data library.
 
    Note: difference between daily and monthly versions:
    - Daily:   F-F_Research_Data_Factors_daily
    - Monthly: F-F_Research_Data_Factors 
 
    The paper (Golez & Jackwerth 2024) uses monthly rf for Table 1:
    "Risk-free rate (rf) is the 1-month Treasury-bill rate."
    """
 
    print("Pulling monthly Fama-French factors...")
 
    ff_data = web.DataReader(
        'F-F_Research_Data_Factors',  # monthly version, no _daily suffix
        'famafrench',
        start_date,
        end_date
    )
 
    # Monthly data is in ff_data[0]
    # Raw data is in percentage format (e.g. 0.5 means 0.5%)
    df = ff_data[0] / 100  # convert to decimal
 
    # Index is Period type (e.g. 1996-01), convert to month-end timestamp
    df = df.reset_index().rename(columns={'Date': 'date'})
    df['date'] = df['date'].dt.to_timestamp('M')
 
    # Rename columns to match project conventions
    df = df.rename(columns={
        'Mkt-RF': 'mkt_rf_monthly',
        'SMB': 'smb_monthly',
        'HML': 'hml_monthly',
        'RF': 'rf_1m_monthly'
    })
 
    print(f"  Got {len(df)} months of data")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  rf_1m range: {df['rf_1m_monthly'].min():.4f} to {df['rf_1m_monthly'].max():.4f}")
 
    return df

if __name__ == "__main__":
    # Pull all data
    df_treasury, df_ff = pull_all_data()
    
    # Create output directory
    filedir = Path(DATA_DIR)
    filedir.mkdir(parents=True, exist_ok=True)
    
    # Save treasury rates
    treasury_path = filedir / "fred_treasury_rates.parquet"
    df_treasury.to_parquet(treasury_path)
    
    # Save Fama-French factors
    ff_path = filedir / "fama_french_factors.parquet"
    df_ff.to_parquet(ff_path)

    df_ff_monthly = pull_fama_french_monthly()
    ff_monthly_path = filedir / "fama_french_monthly.parquet"
    df_ff_monthly.to_parquet(ff_monthly_path)
    print(f"Saved monthly FF factors to {ff_monthly_path}")
    
    print(f"\n{'=' * 70}")
    print("Saved three files:")
    print(f"1. Treasury rates: {treasury_path}")
    print(f"   Shape: {df_treasury.shape}")
    print(f"2. Fama-French daily factors: {ff_path}")
    print(f"   Shape: {df_ff.shape}")
    print(f"3. Fama-French monthly factors: {ff_monthly_path}")
    print(f"   Shape: {df_ff_monthly.shape}")
    print(f"{'=' * 70}")
    
    print(f"\nTreasury rates preview:")
    print(df_treasury.head())
    
    print(f"\nFama-French factors preview:")
    print(df_ff.head())