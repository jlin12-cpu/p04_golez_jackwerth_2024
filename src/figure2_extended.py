"""
Extended Figure 2 from Golez & Jackwerth (2024).

Reproduces Figure 2 (cumulative returns) using all available months
in _data/calc/monthly_returns.parquet, starting in 1996 and extending
beyond the paper's original end date whenever newer data are present.

- Panel A: Raw cumulative returns
- Panel B: Cumulative returns in excess of the risk-free rate
- Panel C: Cumulative returns in excess of Treasury bond returns

Inputs:
    _data/calc/monthly_returns.parquet

Outputs:
    output/figure2_extended/figure2_extended.png
    output/figure2_extended/figure2_extended_series.csv
    output/figure2_extended/figure2_extended_terminal.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output/figure2_extended")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "1996-01-01"


def load_data() -> pd.DataFrame:
    """
    Load monthly returns from 1996 through the latest available date.

    No upper date filter is applied so that the series automatically
    extends as new data becomes available in the pipeline.

    Returns
    -------
    pd.DataFrame
        Monthly return panel with all available dates from 1996 onward.
    """
    file_path = CALC_DIR / "monthly_returns.parquet"
    df = pd.read_parquet(file_path)

    # Only apply start-date filter; keep all later observations
    mask = df["date"] >= START_DATE
    df = df.loc[mask].copy().reset_index(drop=True)

    # Drop first row: strip return unavailable because of lagged holding logic
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

    Parameters
    ----------
    df : pd.DataFrame
        Monthly return panel from load_data().

    Returns
    -------
    pd.DataFrame
        Cumulative wealth indices with an initial $1 row prepended.
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
    Print and save terminal cumulative values.

    No paper benchmark comparison is included here because the extended
    sample goes beyond the paper's original 1996-2022 period.

    Parameters
    ----------
    df_cum : pd.DataFrame
        Cumulative wealth indices from calc_cumulative().

    Returns
    -------
    pd.DataFrame
        Terminal value table.
    """
    end_date = df_cum["date"].iloc[-1].date()

    terminal_df = pd.DataFrame({
        "Series": [
            "Panel A - Strip",
            "Panel A - Market",
            "Panel B - Strip-rf",
            "Panel B - Market-rf",
            "Panel C - Strip-2y",
            "Panel C - Market-10y",
        ],
        "Terminal Value": [
            df_cum["cum_strip"].iloc[-1],
            df_cum["cum_mkt"].iloc[-1],
            df_cum["cum_strip_rf"].iloc[-1],
            df_cum["cum_mkt_rf"].iloc[-1],
            df_cum["cum_strip_2y"].iloc[-1],
            df_cum["cum_mkt_10y"].iloc[-1],
        ],
        "End Date": [end_date] * 6,
    })

    print(f"\nTerminal values (extended sample through {end_date}):")
    print(f"{'Series':25} {'Terminal Value':>15}")
    print("-" * 42)
    for _, row in terminal_df.iterrows():
        print(f"{row['Series']:25} {row['Terminal Value']:>15.2f}")

    out_path = OUTPUT_DIR / "figure2_extended_terminal.csv"
    terminal_df.to_csv(out_path, index=False)
    print(f"\nSaved terminal values to: {out_path}")

    return terminal_df


def plot_figure2_extended(df_cum: pd.DataFrame) -> None:
    """
    Plot extended Figure 2 with three vertically stacked panels.

    Y-axis limits are set dynamically based on the data range to
    accommodate the extended sample period.

    Parameters
    ----------
    df_cum : pd.DataFrame
        Cumulative wealth indices from calc_cumulative().
    """
    cum_cols = [
        "cum_strip", "cum_mkt",
        "cum_strip_rf", "cum_mkt_rf",
        "cum_strip_2y", "cum_mkt_10y"
    ]
    y_max = np.ceil(df_cum[cum_cols].max().max() * 1.1)
    end_date = df_cum["date"].iloc[-1].date()

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
    axes[0].set_ylim(0, y_max)

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
    axes[1].set_ylim(0, y_max)

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
    axes[2].set_ylim(0, y_max)

    plt.suptitle(f"Extended sample: 1996 – {end_date}", fontsize=11, y=0.995)
    plt.tight_layout(rect=[0, 0, 1, 0.98])

    out_path = OUTPUT_DIR / "figure2_extended.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"\nSaved figure to: {out_path}")


if __name__ == "__main__":
    print("=" * 60)
    print("PLOTTING FIGURE 2 (EXTENDED SAMPLE)")
    print("=" * 60)

    df = load_data()
    df_cum = calc_cumulative(df)
    terminal_df = print_terminal_values(df_cum)
    plot_figure2_extended(df_cum)

    out_csv = OUTPUT_DIR / "figure2_extended_series.csv"
    df_cum.to_csv(out_csv, index=False)
    print(f"Saved cumulative series to: {out_csv}")