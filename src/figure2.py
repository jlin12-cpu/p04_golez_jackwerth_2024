"""
Replicate Figure 2 from Golez & Jackwerth (2024).

Figure 2: Cumulative returns (1996-2022)
- Panel A: Raw cumulative returns
- Panel B: Cumulative returns in excess of the risk-free rate
- Panel C: Cumulative returns in excess of Treasury bond returns

Note on automated replication:
    This script uses the monthly return series constructed in our fully
    automated pipeline. As a result, the generated figure may differ from
    the published figure, especially on the strip side, due to differences
    in data availability and return construction. We separately verified in
    notebook diagnostics that the cumulative-return logic itself is correct,
    and that remaining discrepancies are driven by the underlying return
    series rather than by the plotting procedure.

Inputs:
    _data/calc/monthly_returns.parquet

Outputs:
    output/figure2/figure2.png
    output/figure2/figure2_series.csv
    output/figure2/figure2_terminal_comparison.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output/figure2")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "1996-01-01"
END_DATE = "2022-12-31"


def load_data() -> pd.DataFrame:
    """
    Load monthly returns and filter to the paper sample period.

    Returns
    -------
    pd.DataFrame
        Monthly return panel with paper sample period only.
    """
    file_path = CALC_DIR / "monthly_returns.parquet"
    df = pd.read_parquet(file_path)

    # Filter to paper sample period
    mask = (df["date"] >= START_DATE) & (df["date"] <= END_DATE)
    df = df.loc[mask].copy().reset_index(drop=True)

    # Drop first row: strip return is unavailable because of lagged holding logic
    df = df.dropna(subset=["strip_ret", "mkt_ret"]).reset_index(drop=True)

    print(f"Loaded data from: {file_path}")
    print(f"Rows after sample filter and NaN drop: {len(df)}")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")

    return df


def calc_cumulative(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate cumulative wealth indices for all three panels.

    Since the return series in monthly_returns.parquet are stored in
    continuously compounded (log) form, cumulative wealth is computed as:

        wealth_t = exp(sum_{s <= t} r_s)

    This is applied to:
    - raw market / strip returns
    - precomputed excess returns over rf
    - precomputed excess returns over Treasury returns

    Parameters
    ----------
    df : pd.DataFrame
        Monthly return panel after filtering.

    Returns
    -------
    pd.DataFrame
        DataFrame containing cumulative wealth indices for all panels.
    """
    # Panel A: raw cumulative returns
    cum_strip = np.exp(df["strip_ret"].cumsum())
    cum_mkt = np.exp(df["mkt_ret"].cumsum())

    # Panel B: cumulative returns in excess of the risk-free rate
    cum_strip_rf = np.exp(df["strip_ret_rf"].cumsum())
    cum_mkt_rf = np.exp(df["mkt_ret_rf"].cumsum())

    # Panel C: cumulative returns in excess of Treasury returns
    cum_strip_2y = np.exp(df["strip_ret_2y"].cumsum())
    cum_mkt_10y = np.exp(df["mkt_ret_10y"].cumsum())

    df_cum = pd.DataFrame({
        "date": df["date"],
        "cum_strip": cum_strip,
        "cum_mkt": cum_mkt,
        "cum_strip_rf": cum_strip_rf,
        "cum_mkt_rf": cum_mkt_rf,
        "cum_strip_2y": cum_strip_2y,
        "cum_mkt_10y": cum_mkt_10y,
    })

    # Insert initial $1 row at the month-end before the first valid return
    init_date = df["date"].min() - pd.offsets.MonthEnd(1)
    init_row = pd.DataFrame({
        "date": [init_date],
        "cum_strip": [1.0],
        "cum_mkt": [1.0],
        "cum_strip_rf": [1.0],
        "cum_mkt_rf": [1.0],
        "cum_strip_2y": [1.0],
        "cum_mkt_10y": [1.0],
    })

    df_cum = pd.concat([init_row, df_cum], ignore_index=True)

    return df_cum


def print_terminal_values(df_cum: pd.DataFrame) -> pd.DataFrame:
    """
    Print terminal cumulative values and compare them with the paper benchmark.

    Returns
    -------
    pd.DataFrame
        Terminal-value comparison table.
    """
    terminal_df = pd.DataFrame({
        "Series": [
            "Panel A - Strip",
            "Panel A - Market",
            "Panel B - Strip-rf",
            "Panel B - Market-rf",
            "Panel C - Strip-2y",
            "Panel C - Market-10y",
        ],
        "Ours": [
            df_cum["cum_strip"].iloc[-1],
            df_cum["cum_mkt"].iloc[-1],
            df_cum["cum_strip_rf"].iloc[-1],
            df_cum["cum_mkt_rf"].iloc[-1],
            df_cum["cum_strip_2y"].iloc[-1],
            df_cum["cum_mkt_10y"].iloc[-1],
        ],
        "Paper": [6.76, 9.97, 3.97, 5.86, 3.11, 3.45],
    })

    print("\nTerminal value comparison (published paper benchmark):")
    print(f"{'Series':25} {'Ours':>10} {'Paper':>10}")
    print("-" * 47)
    for _, row in terminal_df.iterrows():
        print(f"{row['Series']:25} {row['Ours']:>10.2f} {row['Paper']:>10.2f}")

    out_path = OUTPUT_DIR / "figure2_terminal_comparison.csv"
    terminal_df.to_csv(out_path, index=False)
    print(f"\nSaved terminal comparison to: {out_path}")

    return terminal_df


def plot_figure2(df_cum: pd.DataFrame) -> None:
    """
    Plot Figure 2 with three vertically stacked panels.

    Parameters
    ----------
    df_cum : pd.DataFrame
        Cumulative wealth series for all panels.
    """
    fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)

    # Panel A
    axes[0].plot(
        df_cum["date"], df_cum["cum_strip"],
        color="blue", label="Dividend strip"
    )
    axes[0].plot(
        df_cum["date"], df_cum["cum_mkt"],
        color="orange", linestyle="--", label="Market"
    )
    axes[0].set_ylabel("Investment in $")
    axes[0].set_title("A  Cumulative returns")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    axes[0].set_ylim(0, 13)

    # Panel B
    axes[1].plot(
        df_cum["date"], df_cum["cum_strip_rf"],
        color="blue", label="Dividend strip - rf"
    )
    axes[1].plot(
        df_cum["date"], df_cum["cum_mkt_rf"],
        color="orange", linestyle="--", label="Market - rf"
    )
    axes[1].set_ylabel("Investment in $")
    axes[1].set_title("B  Cumulative returns in excess of the risk-free rate")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    axes[1].set_ylim(0, 13)

    # Panel C
    axes[2].plot(
        df_cum["date"], df_cum["cum_strip_2y"],
        color="blue", label="Dividend strip - Treasury 2y"
    )
    axes[2].plot(
        df_cum["date"], df_cum["cum_mkt_10y"],
        color="orange", linestyle="--", label="Market - Treasury 10y"
    )
    axes[2].set_ylabel("Investment in $")
    axes[2].set_title("C  Cumulative returns in excess of Treasury returns")
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)
    axes[2].set_ylim(0, 13)

    plt.tight_layout()
    out_path = OUTPUT_DIR / "figure2.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"\nSaved figure to: {out_path}")


if __name__ == "__main__":
    print("=" * 60)
    print("PLOTTING FIGURE 2")
    print("=" * 60)

    df = load_data()
    df_cum = calc_cumulative(df)
    terminal_df = print_terminal_values(df_cum)
    plot_figure2(df_cum)

    out_csv = OUTPUT_DIR / "figure2_series.csv"
    df_cum.to_csv(out_csv, index=False)
    print(f"Saved cumulative series to: {out_csv}")