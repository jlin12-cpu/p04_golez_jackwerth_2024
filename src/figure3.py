"""
Replicate Figure 3 from Golez & Jackwerth (2024).

Figure 3: Annualized standard deviation across different holding periods (1996-2022)
- Panel A: Returns in excess of the risk-free rate
- Panel B: Returns in excess of Treasury bond returns

For each holding period h in [1, 6, 12, 18, 24, 30, 36] months, we compute
overlapping h-period log returns by summing h consecutive monthly log returns,
then annualize the standard deviation as:

    annualized_std = std(h-period return) / sqrt(h) * sqrt(12)

The key finding is that strip volatility drops sharply from ~40% at h=1 to
~13% at h=36, while market volatility remains broadly stable at ~16-18%.
This pattern is consistent with measurement error in strip prices estimated
from options data, as discussed in Section 2.2 of the paper.

Note on replication accuracy:
    Strip volatility at h=1 is 40.5% in our replication versus about 32% in the paper,
    consistent with the higher strip return volatility documented in Table 1.
    This difference is attributable to the use of OptionMetrics end-of-day data
    rather than the CBOE intraday data used by the authors. The overall decline
    and convergence pattern are qualitatively similar to the paper.

Inputs:
    _data/calc/monthly_returns.parquet

Outputs:
    output/figure3/figure3.png
    output/figure3/figure3_series.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output/figure3")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "1996-01-01"
END_DATE = "2022-12-31"
HOLDING_PERIODS = [1, 6, 12, 18, 24, 30, 36]


def load_data() -> pd.DataFrame:
    """
    Load monthly returns and filter to the paper sample period.

    Returns
    -------
    pd.DataFrame
        Monthly return panel with columns needed for Figure 3.
    """
    file_path = CALC_DIR / "monthly_returns.parquet"
    df = pd.read_parquet(file_path)

    mask = (df["date"] >= START_DATE) & (df["date"] <= END_DATE)
    df = df.loc[mask].copy().reset_index(drop=True)

    df = df.dropna(
        subset=["strip_ret_rf", "mkt_ret_rf", "strip_ret_2y", "mkt_ret_10y"]
    ).reset_index(drop=True)

    print(f"Loaded data from: {file_path}")
    print(f"Rows after sample filter and NaN drop: {len(df)}")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")

    return df


def calc_annualized_std(series: pd.Series, holding_periods: list) -> list:
    """
    Compute annualized standard deviation for each holding period.

    For holding period h, overlapping h-period log returns are formed by
    summing h consecutive monthly log returns using a rolling window.
    The annualized standard deviation is:

        annualized_std = std(h-period return) / sqrt(h) * sqrt(12)

    Parameters
    ----------
    series : pd.Series
        Monthly log return series.
    holding_periods : list of int
        Holding periods in months.

    Returns
    -------
    list of float
        Annualized standard deviations in percent for each holding period.
    """
    stds = []
    for h in holding_periods:
        h_period_ret = series.rolling(window=h).sum().dropna()
        annualized_std = h_period_ret.std() / np.sqrt(h) * np.sqrt(12)
        stds.append(annualized_std * 100)
    return stds


def calc_all_stds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute annualized standard deviations for all four series
    across all holding periods.
    """
    std_strip_rf = calc_annualized_std(df["strip_ret_rf"], HOLDING_PERIODS)
    std_mkt_rf = calc_annualized_std(df["mkt_ret_rf"], HOLDING_PERIODS)
    std_strip_2y = calc_annualized_std(df["strip_ret_2y"], HOLDING_PERIODS)
    std_mkt_10y = calc_annualized_std(df["mkt_ret_10y"], HOLDING_PERIODS)

    df_stds = pd.DataFrame({
        "holding_period": HOLDING_PERIODS,
        "std_strip_rf": std_strip_rf,
        "std_mkt_rf": std_mkt_rf,
        "std_strip_2y": std_strip_2y,
        "std_mkt_10y": std_mkt_10y,
    })

    print("\nAnnualized standard deviations (%):")
    print(f"{'h':>4}  {'Strip-rf':>10}  {'Mkt-rf':>10}  {'Strip-2y':>10}  {'Mkt-10y':>10}")
    print("-" * 56)
    for _, row in df_stds.iterrows():
        print(
            f"{int(row['holding_period']):>4}m"
            f"  {row['std_strip_rf']:>9.1f}%"
            f"  {row['std_mkt_rf']:>9.1f}%"
            f"  {row['std_strip_2y']:>9.1f}%"
            f"  {row['std_mkt_10y']:>9.1f}%"
        )

    return df_stds


def plot_figure3(df_stds: pd.DataFrame) -> None:
    """
    Plot Figure 3 with two vertically stacked panels.
    """
    fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

    # Panel A: excess over risk-free rate
    axes[0].plot(
        df_stds["holding_period"], df_stds["std_mkt_rf"],
        color="blue", linestyle="-.", label="Market - rf"
    )
    axes[0].plot(
        df_stds["holding_period"], df_stds["std_strip_rf"],
        color="orange", label="Dividend strip - rf"
    )
    axes[0].set_ylabel("Annualized volatility in %")
    axes[0].set_title("A  Returns in excess of the risk-free rate")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    axes[0].set_ylim(10, 45)
    axes[0].set_xticks(HOLDING_PERIODS)

    # Panel B: excess over Treasury returns
    axes[1].plot(
        df_stds["holding_period"], df_stds["std_mkt_10y"],
        color="blue", linestyle="-.", label="Market - Treasury 10y"
    )
    axes[1].plot(
        df_stds["holding_period"], df_stds["std_strip_2y"],
        color="orange", label="Dividend strip - Treasury 2y"
    )
    axes[1].set_ylabel("Annualized volatility in %")
    axes[1].set_title("B  Returns in excess of Treasury bond returns")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    axes[1].set_ylim(10, 45)
    axes[1].set_xlabel("Holding period in months")
    axes[1].set_xticks(HOLDING_PERIODS)

    plt.tight_layout()
    out_path = OUTPUT_DIR / "figure3.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"\nSaved figure to: {out_path}")


if __name__ == "__main__":
    print("=" * 60)
    print("PLOTTING FIGURE 3")
    print("=" * 60)

    df = load_data()
    df_stds = calc_all_stds(df)
    plot_figure3(df_stds)

    out_csv = OUTPUT_DIR / "figure3_series.csv"
    df_stds.to_csv(out_csv, index=False)
    print(f"Saved series to: {out_csv}")