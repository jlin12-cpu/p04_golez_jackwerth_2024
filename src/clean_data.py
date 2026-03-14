"""
This script standardizes and tidies raw data without restricting to a specific
analysis window. Sample period selection is handled in downstream analysis
scripts to support both paper-period replication and updated extensions.
Clean raw data for Golez & Jackwerth (2024) replication.

Inputs:
    _data/optionmetrics_spx_monthly.parquet
    _data/optionmetrics_zero_curve.parquet
    _data/fred_treasury_rates.parquet
    _data/crsp_sp500_daily.parquet

Outputs:
    _data/clean_options.parquet
    _data/clean_zero_curve.parquet
    _data/clean_rates.parquet
    _data/clean_crsp_sp500_monthly.parquet
"""

from pathlib import Path
import pandas as pd
import numpy as np

DATA_DIR = Path("_data")


def clean_zero_curve(df_zero):
    """
    Clean zero curve data.
    - Convert rate from percentage to decimal
    - Calculate tau = days / 365
    """
    df = df_zero.copy()
    df['rate'] = df['rate'] / 100
    df['tau'] = df['days'] / 365
    return df


def clean_rates(df_treas):
    """
    Clean treasury rates.
    - Convert APR to continuously compounded: ln(1 + r)
    - rf_1m already continuous, no conversion needed
    """
    df = df_treas.copy()
    for col in ['treasury_1y', 'treasury_2y', 'treasury_10y']:
        df[col] = np.log(1 + df[col])
    return df


def clean_options(df_opt, df_spx):
    """
    Clean SPX options data.
    - Merge spindx from df_spx
    - Filter: best_bid >= 3 and best_offer >= 3
    - Filter: days_to_maturity >= 90
    - Calculate moneyness = strike / spindx
    - Filter: moneyness in [0.5, 1.5]
    """
    df = df_opt.copy()

    # Merge S&P 500 index level
    df = df.merge(df_spx[['date', 'spindx']], on='date', how='left')

    # Filter by price
    df = df[(df['best_bid'] >= 3) & (df['best_offer'] >= 3)]

    # Filter by maturity
    df = df[df['days_to_maturity'] >= 90]

    # Calculate and filter moneyness
    df['moneyness'] = df['strike'] / df['spindx']
    df = df[(df['moneyness'] >= 0.5) & (df['moneyness'] <= 1.5)]

    return df.reset_index(drop=True)


def clean_spx_monthly(df_spx):
    """
    Aggregate daily CRSP S&P 500 data to monthly frequency and compute
    monthly market log returns. Also infer a realized monthly dividend
    series from the CRSP price index and total return series.

    Steps:
    1. Take the last available spindx within each calendar month
    2. Compound daily vwretd to obtain monthly total simple return
       (vwretd = value-weighted return including dividends)
    3. Compound daily sprtrn to obtain monthly price-only return
       (sprtrn = S&P 500 price return excluding dividends)
    4. Infer realized monthly dividend in index points:
       D_t = S_{t-1} * (R_total - R_price)
    5. Compute monthly log market return:
       mkt_ret = log(1 + R_total)

    Parameters
    ----------
    df_spx : DataFrame
        Daily CRSP S&P 500 data with columns:
            date, spindx, vwretd, vwretx

    Returns
    -------
    DataFrame with columns:
        date         : month-end date
        spindx       : month-end S&P 500 price index
        div_monthly  : implied monthly realized dividend (index points)
        mkt_ret      : monthly log market return (decimal)
    """

    df = df_spx.copy()
    df['year_month'] = df['date'].dt.to_period('M')

    # Step 1: last observed price index in each month
    spindx_monthly = (
        df.groupby('year_month')['spindx']
        .last()
        .reset_index()
    )

    # Step 2: monthly total return from compounded daily vwretd
    # vwretd includes dividends → use for mkt_ret and div calculation
    ret_total = (
        df.groupby('year_month')['vwretd']
        .apply(lambda x: (1 + x).prod() - 1)
        .reset_index()
        .rename(columns={'vwretd': 'ret_total'})
    )

    # Step 3: monthly price-only return from compounded daily vwretx
    # vwretx = value-weighted return excluding dividends
    # same universe as vwretd → consistent dividend extraction
    ret_price = (
        df.groupby('year_month')['vwretx']
        .apply(lambda x: (1 + x).prod() - 1)
        .reset_index()
        .rename(columns={'vwretx': 'ret_price'})
    )

    # Merge all
    df_monthly = (spindx_monthly
        .merge(ret_total, on='year_month', how='inner')
        .merge(ret_price, on='year_month', how='inner')
    )

    # Convert Period to month-end timestamp
    df_monthly['date'] = df_monthly['year_month'].dt.to_timestamp('M')

    # Lagged month-end price index
    df_monthly['spindx_lag'] = df_monthly['spindx'].shift(1)

    # Step 4: implied monthly realized dividend in index points
    # D_t = S_{t-1} * (R_total - R_price)
    df_monthly['div_monthly'] = (
        df_monthly['spindx_lag']
        * (df_monthly['ret_total'] - df_monthly['ret_price'])
    )

    # Step 5: monthly log market return (total, including dividends)
    df_monthly['mkt_ret'] = np.log(1 + df_monthly['ret_total'])

    # Drop first row (no lag) and helper columns
    df_monthly = df_monthly.dropna(subset=['spindx_lag', 'mkt_ret'])
    df_monthly = df_monthly[['date', 'spindx', 'div_monthly', 'mkt_ret']]

    return df_monthly.reset_index(drop=True)

if __name__ == "__main__":
    print("=" * 60)
    print("CLEANING DATA")
    print("=" * 60)

    # Load raw data
    print("\nLoading raw data...")
    df_opt = pd.read_parquet(DATA_DIR / "optionmetrics_spx_monthly.parquet")
    df_zero = pd.read_parquet(DATA_DIR / "optionmetrics_zero_curve.parquet")
    df_treas = pd.read_parquet(DATA_DIR / "fred_treasury_rates.parquet")
    df_spx = pd.read_parquet(DATA_DIR / "crsp_sp500_daily.parquet")

    # Clean
    print("\nCleaning zero curve...")
    df_zero_clean = clean_zero_curve(df_zero)

    print("Cleaning rates...")
    df_treas_clean = clean_rates(df_treas)

    print("Cleaning options...")
    df_opt_clean = clean_options(df_opt, df_spx)

    print("Cleaning S&P 500 to monthly...")
    df_spx_monthly = clean_spx_monthly(df_spx)

    # Save
    print("\nSaving clean data...")
    df_zero_clean.to_parquet(DATA_DIR / "clean_zero_curve.parquet", index=False)
    df_treas_clean.to_parquet(DATA_DIR / "clean_rates.parquet", index=False)
    df_opt_clean.to_parquet(DATA_DIR / "clean_options.parquet", index=False)
    df_spx_monthly.to_parquet(DATA_DIR / "clean_crsp_sp500_monthly.parquet", index=False)

    print("\nDone! Output files:")
    print(f"  clean_zero_curve.parquet: {df_zero_clean.shape}")
    print(f"  clean_rates.parquet: {df_treas_clean.shape}")
    print(f"  clean_options.parquet: {df_opt_clean.shape}")
    print(f"  clean_crsp_sp500_monthly.parquet: {df_spx_monthly.shape}")