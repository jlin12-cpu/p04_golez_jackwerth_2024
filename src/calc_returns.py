"""
Calculate monthly returns for dividend strips and the market.

Following Golez & Jackwerth (2024) Section 2:
- Strip return: ln((P_t + D_t) / P_{t-1})
  where P_t is the price of the held contract at month end,
  and D_t is the realized monthly dividend
- Market return: ln(1 + R_total) where R_total is from vwretd
- Rolling strategy: hold each contract for ~6 months, then roll over

Inputs:
    _data/calc/strip_prices.parquet
    _data/calc/all_strip_prices.parquet
    _data/clean_crsp_sp500_monthly.parquet
    _data/fama_french_monthly.parquet
    _data/crsp_treasury_returns.parquet

Outputs:
    _data/calc/monthly_returns.parquet
"""

from pathlib import Path
import pandas as pd
import numpy as np

DATA_DIR = Path("_data")
CALC_DIR = Path("_data/calc")
CALC_DIR.mkdir(parents=True, exist_ok=True)


def build_strip_panel(strip_prices, all_strip_prices):
    """
    Build the strip return panel following the rolling strategy.

    At each month end:
    - target_exdate: the new contract selected this month
    - holding_exdate: the contract selected last month (currently held)
    - holding_price_t: price of the held contract at this month end
    - holding_price_tminus1: price of the held contract at last month end
                             (= target_price of last month)

    Strip return = ln((holding_price_t + D_t) / holding_price_tminus1)

    Parameters
    ----------
    strip_prices     : DataFrame  target strip prices (one per month)
    all_strip_prices : DataFrame  all (date, exdate) strip prices

    Returns
    -------
    DataFrame with columns:
        date, target_exdate, target_price,
        holding_exdate, holding_price_t, holding_price_tminus1
    """
    # Sort by date
    panel = strip_prices.sort_values("date").copy()
    panel = panel.rename(columns={
        "exdate": "target_exdate",
        "strip_price": "target_price"
    })

    # Holding contract = last month's target contract
    panel["holding_exdate"] = panel["target_exdate"].shift(1)

    # Look up the price of the holding contract at this month end
    # from all_strip_prices
    holding_prices = all_strip_prices.rename(columns={
        "exdate": "holding_exdate",
        "strip_price": "holding_price_t"
    })[["date", "holding_exdate", "holding_price_t"]]

    panel = panel.merge(
        holding_prices,
        on=["date", "holding_exdate"],
        how="left"
    )

    # holding_price_tminus1 = last month's target_price
    panel["holding_price_tminus1"] = panel["target_price"].shift(1)

    return panel.reset_index(drop=True)


def calc_strip_returns(panel, df_spx_monthly):
    """
    Calculate monthly strip log returns.

    strip_ret = ln((holding_price_t + D_t) / holding_price_tminus1)

    Parameters
    ----------
    panel          : DataFrame  strip panel from build_strip_panel()
    df_spx_monthly : DataFrame  monthly S&P 500 data with div_monthly

    Returns
    -------
    DataFrame with strip_ret column added
    """
    # Merge div_monthly using year_month to handle date mismatches
    df = panel.copy()
    df["year_month"] = df["date"].dt.to_period("M")

    spx = df_spx_monthly.copy()
    spx["year_month"] = spx["date"].dt.to_period("M")

    df = df.merge(
        spx[["year_month", "div_monthly"]],
        on="year_month",
        how="left"
    ).drop(columns="year_month")

    # Strip return
    df["strip_ret"] = np.log(
        (df["holding_price_t"] + df["div_monthly"])
        / df["holding_price_tminus1"]
    )

    return df


def build_monthly_returns(panel, df_spx_monthly, df_rf, df_treas):
    """
    Build complete monthly returns table with all excess return variants.

    Columns:
        date
        strip_ret      : raw strip log return
        mkt_ret        : raw market log return
        rf_1m          : 1-month T-bill rate
        treas_2y_ret   : 2-year Treasury log return
        treas_10y_ret  : 10-year Treasury log return
        strip_ret_rf   : strip return minus rf
        mkt_ret_rf     : market return minus rf
        strip_ret_2y   : strip return minus 2Y Treasury
        mkt_ret_10y    : market return minus 10Y Treasury

    Parameters
    ----------
    panel          : DataFrame  strip panel with strip_ret
    df_spx_monthly : DataFrame  monthly S&P 500 data with mkt_ret
    df_rf          : DataFrame  monthly Fama-French factors with rf_1m_monthly
    df_treas       : DataFrame  monthly Treasury returns
    """
    df = panel.copy()
    df["year_month"] = df["date"].dt.to_period("M")

    # Market return
    spx = df_spx_monthly.copy()
    spx["year_month"] = spx["date"].dt.to_period("M")
    df = df.merge(
        spx[["year_month", "mkt_ret"]],
        on="year_month",
        how="left"
    )

    # Risk-free rate
    rf = df_rf.copy()
    rf["year_month"] = rf["date"].dt.to_period("M")
    df = df.merge(
        rf[["year_month", "rf_1m_monthly"]],
        on="year_month",
        how="left"
    )

    # Treasury returns (convert to log returns)
    treas = df_treas.copy()
    treas["year_month"] = treas["date"].dt.to_period("M")
    treas["treas_2y_log"] = np.log(1 + treas["treasury_2y_ret"])
    treas["treas_10y_log"] = np.log(1 + treas["treasury_10y_ret"])
    df = df.merge(
        treas[["year_month", "treas_2y_log", "treas_10y_log"]],
        on="year_month",
        how="left"
    )

    df = df.drop(columns="year_month")

    # Excess returns
    df["strip_ret_rf"]  = df["strip_ret"] - df["rf_1m_monthly"]
    df["mkt_ret_rf"]    = df["mkt_ret"]   - df["rf_1m_monthly"]
    df["strip_ret_2y"]  = df["strip_ret"] - df["treas_2y_log"]
    df["mkt_ret_10y"]   = df["mkt_ret"]   - df["treas_10y_log"]

    # Keep relevant columns
    cols = [
        "date",
        "strip_ret", "mkt_ret",
        "rf_1m_monthly",
        "treas_2y_log", "treas_10y_log",
        "strip_ret_rf", "mkt_ret_rf",
        "strip_ret_2y", "mkt_ret_10y"
    ]
    df = df[cols]

    return df.reset_index(drop=True)


if __name__ == "__main__":
    print("=" * 60)
    print("CALCULATING MONTHLY RETURNS")
    print("=" * 60)

    # Load data
    print("\nLoading data...")
    strip_prices     = pd.read_parquet(CALC_DIR / "strip_prices.parquet")
    all_strip_prices = pd.read_parquet(CALC_DIR / "all_strip_prices.parquet")
    df_spx_monthly   = pd.read_parquet(DATA_DIR / "clean_crsp_sp500_monthly.parquet")
    df_rf            = pd.read_parquet(DATA_DIR / "fama_french_monthly.parquet")
    df_treas         = pd.read_parquet(DATA_DIR / "crsp_treasury_returns.parquet")

    # Build strip panel
    print("\nBuilding strip panel...")
    panel = build_strip_panel(strip_prices, all_strip_prices)
    print(f"  Panel shape: {panel.shape}")
    print(f"  Missing holding_price_t: {panel['holding_price_t'].isna().sum()}")

    # Calculate strip returns
    print("\nCalculating strip returns...")
    panel = calc_strip_returns(panel, df_spx_monthly)
    print(f"  Missing strip_ret: {panel['strip_ret'].isna().sum()}")

    # Build full returns table
    print("\nBuilding monthly returns table...")
    df_returns = build_monthly_returns(panel, df_spx_monthly, df_rf, df_treas)
    print(f"  Shape: {df_returns.shape}")
    print(f"  Missing values:\n{df_returns.isna().sum()}")

    # Quick validation on paper sample period
    print("\n" + "-" * 40)
    print("Validation (1996-2022 paper sample)")
    print("-" * 40)
    mask = (
        (df_returns["date"].dt.year >= 1996) &
        (df_returns["date"].dt.year <= 2022)
    )
    df_paper = df_returns[mask].dropna(subset=["strip_ret", "mkt_ret"])
    print(f"  N: {len(df_paper)}  (paper: 323)")

    strip = df_paper["strip_ret"]
    mkt   = df_paper["mkt_ret"]
    rf    = df_paper["rf_1m_monthly"]

    print(f"\n  Strip:")
    print(f"    Mean (raw):    {strip.mean()*12*100:.2f}%  (paper: 7.10%)")
    print(f"    Std  (raw):    {strip.std()*np.sqrt(12)*100:.2f}%  (paper: 31.98%)")
    print(f"    AR(1):         {strip.autocorr():.2f}  (paper: -0.33)")
    print(f"    Sharpe(-rf):   {((strip-rf).mean()*12)/((strip-rf).std()*np.sqrt(12)):.2f}  (paper: 0.16)")

    print(f"\n  Market:")
    print(f"    Mean (raw):    {mkt.mean()*12*100:.2f}%  (paper: 8.54%)")
    print(f"    Std  (raw):    {mkt.std()*np.sqrt(12)*100:.2f}%  (paper: 15.68%)")
    print(f"    AR(1):         {mkt.autocorr():.2f}  (paper: 0.02)")
    print(f"    Sharpe(-rf):   {((mkt-rf).mean()*12)/((mkt-rf).std()*np.sqrt(12)):.2f}  (paper: 0.42)")

    # Save
    print("\nSaving...")
    df_returns.to_parquet(CALC_DIR / "monthly_returns.parquet", index=False)
    print(f"  Saved to _data/calc/monthly_returns.parquet")
    print(f"  Shape: {df_returns.shape}")