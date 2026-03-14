"""
Calculate dividend strip prices from SPX options.

Following Golez & Jackwerth (2024) Section 2:
- Strip maturity fixed at approximately 2 years
- Options expiring in June or December only
- Strip price estimated from put-call parity using option-implied rate
- Outer product approach for 1996-2003
- Regression approach for 2004 onwards

Inputs:
    _data/clean_options.parquet

Outputs:
    _data/calc/strip_prices.parquet       # target strip prices (one per month)
    _data/calc/all_strip_prices.parquet   # all (date, exdate) strip prices
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


# ============================================================
# Step 1: Select target options
# ============================================================

def get_target_exdate_from_data(date, available_exdates):
    """
    Find target expiration date following the rolling strategy:
    - Jan-Jun → closest December expiry ~1.9 years away
    - Jul-Dec → closest June expiry ~1.9 years away

    Exception (following Golez & Jackwerth 2024):
    If no June/December expiry with tau > 1.3 years is available,
    fall back to the closest June/December with tau > 1.2 years.
    If still not available, use the closest available expiry with
    tau > 1.3 years. This handles the July-August 2013 period when
    June 2015 options were not yet listed.

    Parameters
    ----------
    date             : pd.Timestamp  month-end date
    available_exdates: array-like    all available expiration dates

    Returns
    -------
    pd.Timestamp or None
    """
    date = pd.Timestamp(date)
    month = date.month

    # Determine target month based on rolling rule
    target_month = 12 if month <= 6 else 6

    # Primary: target month with tau > 1.3 years
    candidates = [
        (pd.Timestamp(d), (pd.Timestamp(d) - date).days / 365)
        for d in available_exdates
        if pd.Timestamp(d).month == target_month
    ]
    valid = [(d, t) for d, t in candidates if t > 1.3]

    if valid:
        return min(valid, key=lambda x: abs(x[1] - 1.9))[0]

    # Fallback 1: relax tau threshold to 1.2 for June/December
    valid_relaxed = [(d, t) for d, t in candidates if t > 1.2]
    if valid_relaxed:
        return min(valid_relaxed, key=lambda x: abs(x[1] - 1.5))[0]

    # Fallback 2: any expiry with tau > 1.3 years
    all_candidates = [
        (pd.Timestamp(d), (pd.Timestamp(d) - date).days / 365)
        for d in available_exdates
        if (pd.Timestamp(d) - date).days / 365 > 1.3
    ]
    if not all_candidates:
        return None

    return min(all_candidates, key=lambda x: abs(x[1] - 1.5))[0]


def select_target_options(df_opt):
    """
    For each month-end date, select options with the target expiration date.

    Parameters
    ----------
    df_opt : DataFrame  clean SPX options data

    Returns
    -------
    DataFrame with additional column: target_exdate
    """
    dates = df_opt['date'].unique()
    target_map = {}
    for date in tqdm(dates, desc="Selecting target exdates"):
        available = df_opt[df_opt['date'] == date]['exdate'].unique()
        target_map[date] = get_target_exdate_from_data(pd.Timestamp(date), available)

    df = df_opt.copy()
    df['target_exdate'] = df['date'].map(target_map)
    df_target = df[df['exdate'] == df['target_exdate']].copy()

    return df_target.reset_index(drop=True)


# ============================================================
# Step 2: Calculate option-implied interest rates
# ============================================================

def calc_outer_product_rate(pivot, tau):
    """
    Outer product approach for estimating option-implied interest rate.

    For each pair (Xi, Xj) where Xi > Xj:
        ratio = [(P(Xi) - C(Xi)) - (P(Xj) - C(Xj))] / (Xi - Xj)
        r = -1/tau * ln(ratio)

    Returns median of all valid rates.

    Parameters
    ----------
    pivot : DataFrame  index=strike, columns=['C','P']
    tau   : float      time to maturity in years
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
    Regression approach for estimating option-implied interest rate.

    Model: S - C(X) + P(X) = P_div + beta * X
    beta = exp(-r * tau) → r = -1/tau * ln(beta)

    Parameters
    ----------
    pivot : DataFrame  index=strike, columns=['C','P']
    tau   : float      time to maturity in years
    S     : float      current index level
    """
    y = (S + pivot['P'] - pivot['C']).values
    X = pivot.index.values.reshape(-1, 1)

    model = LinearRegression().fit(X, y)
    beta = model.coef_[0]

    if beta <= 0:
        return np.nan

    return -(1 / tau) * np.log(beta)


def get_implied_rate(group):
    """
    Calculate implied rate for one (date, exdate) group.
    Uses outer product approach for 1996-2003,
    regression approach for 2004 onwards.
    """
    date = group.name[0]
    exdate = group.name[1]

    tau = (exdate - date).days / 365
    S = group['spindx'].iloc[0]

    # Pivot to get Put and Call prices by strike
    pivot = group.pivot_table(
        index='strike',
        columns='cp_flag',
        values='mid_price'
    ).dropna()

    if len(pivot) < 2:
        return np.nan

    if date.year <= 2003:
        return calc_outer_product_rate(pivot, tau)
    else:
        return calc_regression_rate(pivot, tau, S)


# ============================================================
# Step 3: Calculate strip prices
# ============================================================

def calc_strip_price(group, r_implied_map):
    """
    Calculate dividend strip price using put-call parity.

    P_t = S + p(X) - c(X) - X * exp(-r * tau)

    For each put-call pair at the same strike, compute one P_t estimate.
    Return the median across all pairs.

    Parameters
    ----------
    group        : DataFrame  grouped by (date, exdate)
    r_implied_map: dict       {(date, exdate): r_implied}
    """
    date = group.name[0]
    exdate = group.name[1]

    tau = (exdate - date).days / 365
    S = group['spindx'].iloc[0]

    # Get implied rate for this (date, exdate)
    r = r_implied_map.get((date, exdate), np.nan)
    if np.isnan(r):
        return np.nan

    # Pivot to get Put and Call prices by strike
    pivot = group.pivot_table(
        index='strike',
        columns='cp_flag',
        values='mid_price'
    ).dropna()

    if len(pivot) < 1:
        return np.nan

    # For each strike: P_t = S + p(X) - c(X) - X * exp(-r * tau)
    discount = np.exp(-r * tau)
    strip_prices = (
        S
        + pivot['P']
        - pivot['C']
        - pivot.index * discount
    )

    return np.median(strip_prices)


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("CALCULATING STRIP PRICES")
    print("=" * 60)

    # Load clean options
    print("\nLoading clean options...")
    df_opt = pd.read_parquet(DATA_DIR / "clean_options.parquet")
    print(f"  Total options: {len(df_opt):,}")

    # --------------------------------------------------------
    # Part A: Target strip prices (one per month)
    # --------------------------------------------------------
    print("\n" + "-" * 40)
    print("Part A: Target strip prices")
    print("-" * 40)

    # Select target options
    print("\nSelecting target options...")
    df_target = select_target_options(df_opt)
    print(f"  Target options: {len(df_target):,}")
    print(f"  Unique dates: {df_target['date'].nunique()}")

    # Calculate implied rates for target options
    print("\nCalculating implied rates...")
    tqdm.pandas(desc="Implied rates")
    target_rates = df_target.groupby(['date', 'exdate']).progress_apply(
        get_implied_rate, include_groups=False
    ).reset_index()
    target_rates.columns = ['date', 'exdate', 'r_implied']
    print(f"  Missing rates: {target_rates['r_implied'].isna().sum()}")

    # Build implied rate lookup
    r_map = {
        (row['date'], row['exdate']): row['r_implied']
        for _, row in target_rates.iterrows()
    }

    # Calculate strip prices
    print("\nCalculating strip prices...")
    tqdm.pandas(desc="Strip prices")
    strip_prices = df_target.groupby(['date', 'exdate']).progress_apply(
        calc_strip_price, r_implied_map=r_map, include_groups=False
    ).reset_index()
    strip_prices.columns = ['date', 'exdate', 'strip_price']
    strip_prices['tau'] = (
        strip_prices['exdate'] - strip_prices['date']
    ).dt.days / 365
    print(f"  Missing prices: {strip_prices['strip_price'].isna().sum()}")
    print(f"  strip_price range: {strip_prices['strip_price'].min():.2f} "
          f"to {strip_prices['strip_price'].max():.2f}")

    # --------------------------------------------------------
    # Part B: All (date, exdate) strip prices
    # --------------------------------------------------------
    print("\n" + "-" * 40)
    print("Part B: All strip prices")
    print("-" * 40)

    # Calculate implied rates for all (date, exdate)
    print("\nCalculating implied rates for all groups...")
    tqdm.pandas(desc="All implied rates")
    all_rates = df_opt.groupby(['date', 'exdate']).progress_apply(
        get_implied_rate, include_groups=False
    ).reset_index()
    all_rates.columns = ['date', 'exdate', 'r_implied']
    print(f"  Missing rates: {all_rates['r_implied'].isna().sum()}")

    # Build full implied rate lookup
    all_r_map = {
        (row['date'], row['exdate']): row['r_implied']
        for _, row in all_rates.iterrows()
    }

    # Calculate all strip prices
    print("\nCalculating all strip prices...")
    tqdm.pandas(desc="All strip prices")
    all_strip_prices = df_opt.groupby(['date', 'exdate']).progress_apply(
        calc_strip_price, r_implied_map=all_r_map, include_groups=False
    ).reset_index()
    all_strip_prices.columns = ['date', 'exdate', 'strip_price']
    all_strip_prices['tau'] = (
        all_strip_prices['exdate'] - all_strip_prices['date']
    ).dt.days / 365
    print(f"  Missing prices: {all_strip_prices['strip_price'].isna().sum()}")

    # Merge implied rates back
    strip_prices = strip_prices.merge(
        target_rates,
        on=["date", "exdate"],
        how="left"
    )

    all_strip_prices = all_strip_prices.merge(
        all_rates,
        on=["date", "exdate"],
        how="left"
    )

    # Sort for cleaner downstream use
    strip_prices = strip_prices.sort_values("date").reset_index(drop=True)
    all_strip_prices = all_strip_prices.sort_values(["date", "exdate"]).reset_index(drop=True)

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------
    
    print("\nSaving results...")
    strip_prices.to_parquet(CALC_DIR / "strip_prices.parquet", index=False)
    all_strip_prices.to_parquet(CALC_DIR / "all_strip_prices.parquet", index=False)

    print("\nDone! Output files:")
    print(f"  strip_prices.parquet: {strip_prices.shape}")
    print(f"  all_strip_prices.parquet: {all_strip_prices.shape}")