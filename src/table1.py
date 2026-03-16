"""
Replicate Table 1 from Golez & Jackwerth (2024).

Table 1: Monthly returns (annualized)
- Raw returns
- Returns in excess of the risk-free rate
- Returns in excess of Treasury bond returns

Inputs:
    _data/calc/monthly_returns.parquet

Outputs:
    _output/table1.csv
    _output/table1.tex
"""

from pathlib import Path
import pandas as pd
import numpy as np

CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Paper sample period
START_YEAR = 1996
END_YEAR = 2022


def calc_sharpe(excess_ret: pd.Series) -> float:
    """
    Annualized Sharpe ratio from monthly excess returns.

    Sharpe = (mean * 12) / (std * sqrt(12))
    """
    excess_ret = excess_ret.dropna()
    if len(excess_ret) < 2 or excess_ret.std() == 0:
        return np.nan
    return (excess_ret.mean() * 12) / (excess_ret.std() * np.sqrt(12))


def calc_ar1(ret: pd.Series) -> float:
    """
    AR(1) autocorrelation coefficient.
    """
    ret = ret.dropna()
    if len(ret) < 2:
        return np.nan
    return ret.autocorr(lag=1)


def summarize_series(ret: pd.Series, sharpe_series: pd.Series | None = None) -> dict:
    """
    Summarize a monthly return series into annualized Table 1 statistics.

    Parameters
    ----------
    ret : pd.Series
        Monthly return series to summarize.
    sharpe_series : pd.Series or None
        Monthly excess return series used for Sharpe ratio.
        If None, Sharpe ratio is reported as NaN.

    Returns
    -------
    dict
        Dictionary with Mean, Std. dev., Sharpe ratio, AR(1), N.
    """
    ret = ret.dropna()

    if sharpe_series is not None:
        sharpe_series = sharpe_series.dropna()

    return {
        "Mean": ret.mean() * 12 * 100,
        "Std. dev.": ret.std() * np.sqrt(12) * 100,
        "Sharpe ratio": calc_sharpe(sharpe_series) if sharpe_series is not None else np.nan,
        "AR(1)": calc_ar1(ret),
        "N": len(ret),
    }


def build_table1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build Table 1 summary statistics.

    Rows:
        Market
        Strip
        Market - rf
        Strip - rf
        Market - 10Y
        Strip - 2Y

    Columns:
        Mean
        Std. dev.
        Sharpe ratio
        AR(1)
        N

    Parameters
    ----------
    df : pd.DataFrame
        Monthly returns panel for the paper sample period.

    Returns
    -------
    pd.DataFrame
        Numeric Table 1 summary table.
    """
    results = {}

    # Raw returns
    mkt = df["mkt_ret"].dropna()
    strip = df["strip_ret"].dropna()

    results["Market"] = summarize_series(mkt, sharpe_series=None)
    results["Strip"] = summarize_series(strip, sharpe_series=None)

    # Excess returns over risk-free rate
    mkt_rf = df["mkt_ret_rf"].dropna()
    strip_rf = df["strip_ret_rf"].dropna()

    results["Market - rf"] = summarize_series(mkt_rf, sharpe_series=mkt_rf)
    results["Strip - rf"] = summarize_series(strip_rf, sharpe_series=strip_rf)

    # Excess returns over Treasury bond returns
    mkt_10y = df["mkt_ret_10y"].dropna()
    strip_2y = df["strip_ret_2y"].dropna()

    results["Market - 10Y"] = summarize_series(mkt_10y, sharpe_series=mkt_10y)
    results["Strip - 2Y"] = summarize_series(strip_2y, sharpe_series=strip_2y)

    df_table = pd.DataFrame(results).T
    df_table = df_table[["Mean", "Std. dev.", "Sharpe ratio", "AR(1)", "N"]]
    df_table.index.name = "Series"

    return df_table


def format_table1(df_table: pd.DataFrame) -> pd.DataFrame:
    """
    Format Table 1 for display and export.

    - Mean and Std. dev. shown as percentages
    - Sharpe ratio and AR(1) shown with 2 decimals
    - N shown as integer
    """
    df_fmt = df_table.copy()

    for col in ["Mean", "Std. dev."]:
        df_fmt[col] = df_fmt[col].apply(
            lambda x: f"{x:.2f}\\%" if pd.notna(x) else ""
        )

    df_fmt["Sharpe ratio"] = df_fmt["Sharpe ratio"].apply(
        lambda x: f"{x:.2f}" if pd.notna(x) else ""
    )

    df_fmt["AR(1)"] = df_fmt["AR(1)"].apply(
        lambda x: f"{x:.2f}" if pd.notna(x) else ""
    )

    df_fmt["N"] = df_fmt["N"].apply(
        lambda x: f"{int(x)}" if pd.notna(x) else ""
    )

    df_fmt.index.name = "Series"
    return df_fmt


def to_latex(df_fmt: pd.DataFrame) -> str:
    latex = df_fmt.to_latex(
        na_rep="",
        caption="Monthly returns (annualized).",
        bold_rows=False,
        escape=False,
    )
    return latex


if __name__ == "__main__":
    print("=" * 60)
    print("TABLE 1: MONTHLY RETURNS (ANNUALIZED)")
    print("=" * 60)

    # Load monthly returns
    print("\nLoading monthly returns...")
    df_returns = pd.read_parquet(CALC_DIR / "monthly_returns.parquet")

    # Filter to paper sample period
    mask = (
        (df_returns["date"].dt.year >= START_YEAR) &
        (df_returns["date"].dt.year <= END_YEAR)
    )
    df = df_returns.loc[mask].copy()

    print(f"  Sample period: {START_YEAR}-{END_YEAR}")
    print(f"  Rows in sample: {len(df)}")

    # Build numeric table
    print("\nBuilding Table 1...")
    df_table = build_table1(df)

    print("\nRaw numeric table:")
    print(df_table.round(3).to_string())

    # Build formatted table
    df_fmt = format_table1(df_table)

    print("\n" + "=" * 60)
    print("TABLE 1 (formatted)")
    print("=" * 60)
    print(df_fmt.to_string())

    # Comparison with paper
    print("\n" + "=" * 60)
    print("COMPARISON WITH PAPER")
    print("=" * 60)

    paper = {
        "Market": {
            "Mean": 8.54, "Std. dev.": 15.68, "Sharpe ratio": np.nan, "AR(1)": 0.02
        },
        "Strip": {
            "Mean": 7.10, "Std. dev.": 31.98, "Sharpe ratio": np.nan, "AR(1)": -0.33
        },
        "Market - rf": {
            "Mean": 6.57, "Std. dev.": 15.71, "Sharpe ratio": 0.42, "AR(1)": 0.02
        },
        "Strip - rf": {
            "Mean": 5.12, "Std. dev.": 31.99, "Sharpe ratio": 0.16, "AR(1)": -0.33
        },
        "Market - 10Y": {
            "Mean": 4.60, "Std. dev.": 18.08, "Sharpe ratio": 0.25, "AR(1)": 0.08
        },
        "Strip - 2Y": {
            "Mean": 4.22, "Std. dev.": 31.98, "Sharpe ratio": 0.13, "AR(1)": -0.33
        },
    }

    df_paper = pd.DataFrame(paper).T[["Mean", "Std. dev.", "Sharpe ratio", "AR(1)"]]
    df_paper.index.name = "Series"

    print("\nOurs:")
    print(df_table[["Mean", "Std. dev.", "Sharpe ratio", "AR(1)"]].round(2).to_string())

    print("\nPaper:")
    print(df_paper.round(2).to_string())

    # Save outputs
    print("\nSaving...")
    df_fmt.to_csv(OUTPUT_DIR / "table1.csv")
    with open(OUTPUT_DIR / "table1.tex", "w") as f:
        f.write(to_latex(df_fmt))

    print("  Saved to _output/table1.csv")
    print("  Saved to _output/table1.tex")