"""
Calculate option-implied interest rates from SPX options.

Following Golez & Jackwerth (2024) Section 2.1:
- Outer Product Approach: 1996-2003
- Regression Approach: 2004-2022

Inputs:
    _data/clean_options.parquet
    _data/clean_zero_curve.parquet

Outputs:
    _data/calc/implied_rates.parquet       # all maturities
    _data/calc/implied_rates_1y.parquet    # 1-year constant maturity
    _data/calc/zero_curve_1y.parquet       # 1-year zero curve
"""

from pathlib import Path
import pandas as pd
import numpy as np
from itertools import combinations
from sklearn.linear_model import LinearRegression
from tqdm import tqdm

DATA_DIR = Path("_data")
CALC_DIR = Path("_data/calc")
CALC_DIR.mkdir(parents=True, exist_ok=True)


def calc_outer_product_rate(pivot, tau):
    """
    Outer Product Approach for estimating option-implied interest rate.

    For each pair (Xi, Xj) where Xi > Xj:
        ratio = [(P(Xi)-C(Xi)) - (P(Xj)-C(Xj))] / (Xi - Xj)
        r = -1/tau * ln(ratio)

    Returns median of all valid rates.
    """
    strikes = sorted(pivot.index.values)
    rates = []

    for Xj, Xi in combinations(strikes, 2):
        num = (pivot.loc[Xi, 'P'] - pivot.loc[Xi, 'C']) - \
              (pivot.loc[Xj, 'P'] - pivot.loc[Xj, 'C'])
        den = Xi - Xj
        ratio = num / den

        if ratio > 0:
            r = -(1 / tau) * np.log(ratio)
            rates.append(r)

    return np.median(rates) if rates else np.nan


def calc_regression_rate(pivot, tau, S):
    """
    Regression Approach for estimating option-implied interest rate.

    Model: S - C(X) + P(X) = P_div + beta * X
    beta = exp(-r * tau)
    r = -1/tau * ln(beta)
    """
    y = (S + pivot['P'] - pivot['C']).values
    X = pivot.index.values.reshape(-1, 1)

    model = LinearRegression().fit(X, y)
    beta = model.coef_[0]

    if beta <= 0:
        return np.nan

    return -(1 / tau) * np.log(beta)


def get_rate_for_group(group):
    """
    Calculate implied rate for one (date, exdate) group.
    Uses outer product approach for 1996-2003,
    regression approach for 2004 onwards.
    """
    date = group['date'].iloc[0]
    tau = (group['exdate'].iloc[0] - date).days / 365
    S = group['spindx'].iloc[0]

    # Pivot to get Put and Call prices by strike
    pivot = group.pivot_table(
        index='strike',
        columns='cp_flag',
        values='mid_price'
    ).dropna()

    # Need at least 2 strikes
    if len(pivot) < 2:
        return np.nan

    if date.year <= 2003:
        return calc_outer_product_rate(pivot, tau)
    else:
        return calc_regression_rate(pivot, tau, S)


def interpolate_1y(group):
    """
    Linearly interpolate implied rates to 1-year constant maturity.
    """
    group = group.sort_values('tau')
    below = group[group['tau'] <= 1]
    above = group[group['tau'] > 1]

    if len(below) == 0 or len(above) == 0:
        return pd.Series({'r_1y': np.nan})

    r1 = below.iloc[-1]
    r2 = above.iloc[0]

    if r1['tau'] == r2['tau']:
        return pd.Series({'r_1y': r1['r_implied']})

    weight = (1 - r1['tau']) / (r2['tau'] - r1['tau'])
    r_interp = r1['r_implied'] + weight * (r2['r_implied'] - r1['r_implied'])

    return pd.Series({'r_1y': r_interp})


def interpolate_zero_1y(group):
    """
    Linearly interpolate zero curve to 1-year constant maturity.
    """
    group = group.sort_values('tau')
    below = group[group['tau'] <= 1]
    above = group[group['tau'] > 1]

    if len(below) == 0 or len(above) == 0:
        return pd.Series({'zero_1y': np.nan})

    r1 = below.iloc[-1]
    r2 = above.iloc[0]

    if r1['tau'] == r2['tau']:
        return pd.Series({'zero_1y': r1['rate']})

    weight = (1 - r1['tau']) / (r2['tau'] - r1['tau'])
    r_interp = r1['rate'] + weight * (r2['rate'] - r1['rate'])

    return pd.Series({'zero_1y': r_interp})


def get_zero_curve_monthly(df_zero):
    """
    Filter zero curve to month-end dates.
    """
    df = df_zero.copy()
    df['year_month'] = df['date'].dt.to_period('M')
    last_days = df.groupby('year_month')['date'].max().reset_index()
    last_days.columns = ['year_month', 'last_date']

    df_monthly = df.merge(last_days, on='year_month')
    df_monthly = df_monthly[
        df_monthly['date'] == df_monthly['last_date']
    ][['date', 'zero_1y']].reset_index(drop=True)

    return df_monthly


if __name__ == "__main__":
    print("=" * 60)
    print("CALCULATING IMPLIED RATES")
    print("=" * 60)

    # Load clean data
    print("\nLoading clean data...")
    df_opt = pd.read_parquet(DATA_DIR / "clean_options.parquet")
    df_zero = pd.read_parquet(DATA_DIR / "clean_zero_curve.parquet")

    # Calculate implied rates
    print("\nCalculating implied rates...")
    tqdm.pandas(desc="Processing groups")
    results = df_opt.groupby(['date', 'exdate']).progress_apply(get_rate_for_group)

    # Clean up results
    df_rates = results.reset_index()
    df_rates.columns = ['date', 'exdate', 'r_implied']
    df_rates['tau'] = (df_rates['exdate'] - df_rates['date']).dt.days / 365
    df_rates = df_rates.dropna(subset=['r_implied'])

    # Interpolate to 1-year maturity
    print("\nInterpolating to 1-year maturity...")
    df_1y = (
        df_rates.groupby('date')
        .apply(interpolate_1y, include_groups=False)
        .dropna()
        .reset_index()
    )

    # Interpolate zero curve to 1-year maturity
    print("\nInterpolating zero curve to 1-year maturity...")
    df_zero_1y = (
        df_zero.groupby('date')
        .apply(interpolate_zero_1y, include_groups=False)
        .dropna()
        .reset_index()
    )

    # Filter zero curve to month-end
    df_zero_monthly = get_zero_curve_monthly(df_zero_1y)

    # Save
    print("\nSaving results...")
    df_rates.to_parquet(CALC_DIR / "implied_rates.parquet", index=False)
    df_1y.to_parquet(CALC_DIR / "implied_rates_1y.parquet", index=False)
    df_zero_monthly.to_parquet(CALC_DIR / "zero_curve_1y.parquet", index=False)
    
    print("\nDone! Output files:")
    print(f"  _data/calc/implied_rates.parquet: {df_rates.shape}")
    print(f"  _data/calc/implied_rates_1y.parquet: {df_1y.shape}")
    print(f"  _data/calc/zero_curve_1y.parquet: {df_zero_monthly.shape}")