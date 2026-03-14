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

    # Save
    print("\nSaving clean data...")
    df_zero_clean.to_parquet(DATA_DIR / "clean_zero_curve.parquet", index=False)
    df_treas_clean.to_parquet(DATA_DIR / "clean_rates.parquet", index=False)
    df_opt_clean.to_parquet(DATA_DIR / "clean_options.parquet", index=False)

    print("\nDone! Output files:")
    print(f"  clean_zero_curve.parquet: {df_zero_clean.shape}")
    print(f"  clean_rates.parquet: {df_treas_clean.shape}")
    print(f"  clean_options.parquet: {df_opt_clean.shape}")