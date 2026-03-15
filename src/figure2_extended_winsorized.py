"""
Winsorized extended Figure 2 from Golez & Jackwerth (2024).

This script reproduces the extended-sample version of Figure 2 using all
available months in _data/calc/monthly_returns.parquet, but applies a
post-2022 robustness adjustment to the strip-return series.

Specifically, strip log returns are clipped to [-cap, cap] only for dates
after 2022-12-31. This is used as a robustness check for late-sample
instability in the estimated strip-price series. The paper sample
(1996-2022) is left unchanged.

- Panel A: Raw cumulative returns
- Panel B: Cumulative returns in excess of the risk-free rate
- Panel C: Cumulative returns in excess of Treasury bond returns

Inputs:
    _data/calc/monthly_returns.parquet

Outputs:
    output/figure2_extended_winsorized/figure2_extended_winsorized.png
    output/figure2_extended_winsorized/figure2_extended_winsorized_series.csv
    output/figure2_extended_winsorized/figure2_extended_winsorized_terminal.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output/figure2_extended_winsorized")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "1996-01-01"
PAPER_END = pd.Timestamp("2022-12-31")
CAP = 0.50  # cap on strip log returns


def load_data() -> pd.DataFrame:
    """
    Load monthly returns from 1996 through the latest available date.
    """
    file_path = CALC_DIR / "monthly_returns.parquet"
    df = pd.read_parquet(file_path)

    mask = df["date"] >= START_DATE
    df = df.loc[mask].copy().reset_index(drop=True)

    # Drop first row: strip return unavailable because of lagged holding logic
    df = df.dropna(subset=["strip_ret", "mkt_ret"]).reset_index(drop=True)

    print(f"Loaded data from: {file_path}")
    print(f"Rows after sample filter and NaN drop: {len(df)}")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")

    return df


def winsorize_strip_extended(df: pd.DataFrame, cap: float = CAP) -> pd.DataFrame:
    """
    Clip post-2022 strip log returns to [-cap, cap] as a robustness check.

    Only strip-side returns are modified, and only after the paper sample end.
    The paper sample itself is left unchanged.
    """
    df = df.copy()
    ext = df["date"] > PAPER_END

    original_strip = df.loc[ext, "strip_ret"].copy()
    n_clipped = (original_strip.abs() > cap).sum()

    df.loc[ext, "strip_ret"] = original_strip.clip(-cap, cap)
    df.loc[ext, "strip_ret_rf"] = df.loc[ext, "strip_ret"] - df.loc[ext, "rf_1m_monthly"]
    df.loc[ext, "strip_ret_2y"] = df.loc[ext, "strip_ret"] - df.loc[ext, "treas_2y_log"]

    print(f"Clipped {n_clipped} post-2022 strip return(s) to [-{cap:.2f}, {cap:.2f}]")

    return df


def calc_cumulative(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate cumulative wealth indices for all three panels.
    """
    cum_strip = np.exp(df["strip_ret"].cumsum())
    cum_mkt = np.exp(df["mkt_ret"].cumsum())

    cum_strip_rf = np.exp(df["strip_ret_rf"].cumsum())
    cum_mkt_rf = np.exp(df["mkt_ret_rf"].cumsum())

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

    return pd.concat([init_row, df_cum], ignore_index=True)


def save_terminal_values(df_cum: pd.DataFrame) -> pd.DataFrame:
    """
    Print and save terminal cumulative values.
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

    print(f"\nTerminal values (winsorized extended sample through {end_date}):")
    print(f"{'Series':25} {'Terminal Value':>15}")
    print("-" * 42)
    for _, row in terminal_df.iterrows():
        print(f"{row['Series']:25} {row['Terminal Value']:>15.2f}")

    out_path = OUTPUT_DIR / "figure2_extended_winsorized_terminal.csv"
    terminal_df.to_csv(out_path, index=False)
    print(f"\nSaved terminal values to: {out_path}")

    return terminal_df


def plot_figure(df_cum: pd.DataFrame) -> None:
    """
    Plot winsorized extended Figure 2.
    """
    cum_cols = [
        "cum_strip", "cum_mkt",
        "cum_strip_rf", "cum_mkt_rf",
        "cum_strip_2y", "cum_mkt_10y"
    ]
    y_max = np.ceil(df_cum[cum_cols].max().max() * 1.1)
    end_date = df_cum["date"].iloc[-1].date()

    fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)

    axes[0].plot(df_cum["date"], df_cum["cum_strip"], color="blue", label="Dividend strip")
    axes[0].plot(df_cum["date"], df_cum["cum_mkt"], color="orange", linestyle="--", label="Market")
    axes[0].set_ylabel("Investment in $")
    axes[0].set_title("A  Cumulative returns")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    axes[0].set_ylim(0, y_max)

    axes[1].plot(df_cum["date"], df_cum["cum_strip_rf"], color="blue", label="Dividend strip - rf")
    axes[1].plot(df_cum["date"], df_cum["cum_mkt_rf"], color="orange", linestyle="--", label="Market - rf")
    axes[1].set_ylabel("Investment in $")
    axes[1].set_title("B  Cumulative returns in excess of the risk-free rate")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    axes[1].set_ylim(0, y_max)

    axes[2].plot(df_cum["date"], df_cum["cum_strip_2y"], color="blue", label="Dividend strip - Treasury 2y")
    axes[2].plot(df_cum["date"], df_cum["cum_mkt_10y"], color="orange", linestyle="--", label="Market - Treasury 10y")
    axes[2].set_ylabel("Investment in $")
    axes[2].set_title("C  Cumulative returns in excess of Treasury returns")
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)
    axes[2].set_ylim(0, y_max)

    plt.suptitle(
        f"Winsorized extended sample: 1996 – {end_date} (post-2022 strip cap = ±{CAP:.2f})",
        fontsize=11,
        y=0.995,
    )
    plt.tight_layout(rect=[0, 0, 1, 0.98])

    out_path = OUTPUT_DIR / "figure2_extended_winsorized.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"\nSaved figure to: {out_path}")


if __name__ == "__main__":
    print("=" * 60)
    print("PLOTTING FIGURE 2 (EXTENDED, WINSORIZED)")
    print("=" * 60)

    df_raw = load_data()
    df_winsor = winsorize_strip_extended(df_raw, cap=CAP)
    df_cum = calc_cumulative(df_winsor)
    save_terminal_values(df_cum)
    plot_figure(df_cum)

    out_csv = OUTPUT_DIR / "figure2_extended_winsorized_series.csv"
    df_cum.to_csv(out_csv, index=False)
    print(f"Saved cumulative series to: {out_csv}")