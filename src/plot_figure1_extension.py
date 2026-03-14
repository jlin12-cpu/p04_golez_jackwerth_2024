"""
Extension of Figure 1 from Golez & Jackwerth (2024).

Extends the sample period beyond the original 2022 end date
to show the most recent available data.

Inputs:
    _data/calc/implied_rates_1y.parquet
    _data/calc/zero_curve_1y.parquet
    _data/clean_rates.parquet

Outputs:
    output/figure1_extension/figure1_extension.png
    output/figure1_extension/figure1_extension_series.csv
    output/figure1_extension/figure1_extension_summary.csv
    output/figure1_extension/figure1_extension_diagnostics.csv
    output/figure1_extension/figure1_extension_robustness.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DATA_DIR = Path("_data")
CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output/figure1_extension")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Original paper sample period
PAPER_START = "1996-01-01"
PAPER_END = "2022-12-31"

def load_data():
    """
    Load and merge all three interest rate series
    for the extended sample period.
    """
    df_imp = pd.read_parquet(CALC_DIR / "implied_rates_1y.parquet")
    df_zero = pd.read_parquet(CALC_DIR / "zero_curve_1y.parquet")
    df_treas = pd.read_parquet(DATA_DIR / "clean_rates.parquet")
    df_treas = df_treas[['date', 'treasury_1y']].dropna()

    df_all = (
        df_imp
        .merge(df_zero, on='date', how='inner')
        .merge(df_treas, on='date', how='inner')
        .sort_values('date')
    )

    df_all = df_all[df_all['date'] >= PAPER_START].reset_index(drop=True)

    ext_end = df_all['date'].max()
    print(f"Extended sample end date (from merged data): {ext_end}")

    return df_all


def print_summary(df_all):
    """
    Print summary statistics comparing original and extended samples.
    """
    # Original sample
    df_orig = df_all[df_all['date'] <= PAPER_END]
    # Extended period only
    df_ext = df_all[df_all['date'] > PAPER_END]

    def calc_stats(df):
        if df.empty:
            return np.nan, np.nan
        diff_bp = (df['r_1y'] - df['zero_1y']) * 10000
        rel = ((df['r_1y'] - df['zero_1y']).mean() /
               df['zero_1y'].mean()) * 100
        return diff_bp.mean(), rel

    orig_bp, orig_rel = calc_stats(df_orig)
    ext_bp, ext_rel = calc_stats(df_ext)
    full_bp, full_rel = calc_stats(df_all)

    ext_start = pd.Timestamp(PAPER_END) + pd.offsets.Day(1)
    ext_end = df_all['date'].max()

    print("\nSummary Statistics:")
    print(f"\n  Original sample (1996-2022):")
    print(f"    Average difference: {orig_bp:.2f} bp  [Paper: 7 bp]")
    print(f"    Relative difference: {orig_rel:.2f}%  [Paper: 2.82%]")
    print(f"\n  Extended period ({ext_start.date()} to {ext_end.date()}):")
    print(f"    Average difference: {ext_bp:.2f} bp")
    print(f"    Relative difference: {ext_rel:.2f}%")
    print(f"\n  Full extended sample (1996 to {ext_end.year}):")
    print(f"    Average difference: {full_bp:.2f} bp")
    print(f"    Relative difference: {full_rel:.2f}%")

    # Save summary
    summary_df = pd.DataFrame({
        "period": [
            "original_1996_2022",
            f"extended_{ext_start.year}_{ext_end.year}",
            f"full_1996_{ext_end.year}",
        ],
        "avg_diff_bp": [orig_bp, ext_bp, full_bp],
        "rel_diff_pct": [orig_rel, ext_rel, full_rel],
        "paper_avg_diff_bp": [7.00, None, None],
        "paper_rel_diff_pct": [2.82, None, None],
    })

    summary_df.to_csv(
        OUTPUT_DIR / "figure1_extension_summary.csv", index=False
    )
    print("\nSaved to output/figure1_extension_summary.csv")


def save_extension_diagnostics(df_all):
    """
    Save month-by-month diagnostics for the extended period only.
    """
    df_ext = df_all[df_all['date'] > PAPER_END].copy()

    if df_ext.empty:
        print("\nNo extended-period observations found.")
        return

    df_ext['diff_bp'] = (df_ext['r_1y'] - df_ext['zero_1y']) * 10000
    df_ext['rel_diff_pct'] = (
        (df_ext['r_1y'] - df_ext['zero_1y']) / df_ext['zero_1y']
    ) * 100

    print("\nExtended-period monthly diagnostics:")
    print(df_ext[['date', 'r_1y', 'zero_1y', 'treasury_1y', 'diff_bp', 'rel_diff_pct']])

    print("\nExtended-period diff summary:")
    print(df_ext['diff_bp'].describe())

    df_ext[['date', 'r_1y', 'zero_1y', 'treasury_1y', 'diff_bp', 'rel_diff_pct']].to_csv(
        OUTPUT_DIR / "figure1_extension_diagnostics.csv",
        index=False
    )
    print("\nSaved to output/figure1_extension/figure1_extension_diagnostics.csv")

def save_extension_robustness_check(df_all):
    """
    Check whether the extension result is still negative after removing
    the most extreme month in the extended sample.
    """
    df_ext = df_all[df_all['date'] > PAPER_END].copy()

    if df_ext.empty:
        print("\nNo extended-period observations found for robustness check.")
        return

    df_ext['diff_bp'] = (df_ext['r_1y'] - df_ext['zero_1y']) * 10000
    df_ext['rel_diff_pct'] = (
        (df_ext['r_1y'] - df_ext['zero_1y']) / df_ext['zero_1y']
    ) * 100

    # Identify the most negative month
    worst_idx = df_ext['diff_bp'].idxmin()
    worst_row = df_ext.loc[worst_idx]

    # Remove that month
    df_ext_wo = df_ext.drop(index=worst_idx).copy()

    avg_diff_bp_all = df_ext['diff_bp'].mean()
    avg_rel_all = (
        (df_ext['r_1y'] - df_ext['zero_1y']).mean() / df_ext['zero_1y'].mean()
    ) * 100

    avg_diff_bp_wo = df_ext_wo['diff_bp'].mean()
    avg_rel_wo = (
        (df_ext_wo['r_1y'] - df_ext_wo['zero_1y']).mean() / df_ext_wo['zero_1y'].mean()
    ) * 100

    print("\nExtension robustness check:")
    print(
        f"  Worst month: {worst_row['date'].date()} "
        f"(diff = {worst_row['diff_bp']:.2f} bp, "
        f"rel diff = {worst_row['rel_diff_pct']:.2f}%)"
    )
    print(f"  With all extension months: avg diff = {avg_diff_bp_all:.2f} bp, rel diff = {avg_rel_all:.2f}%")
    print(f"  Without worst month:       avg diff = {avg_diff_bp_wo:.2f} bp, rel diff = {avg_rel_wo:.2f}%")

    robustness_df = pd.DataFrame({
        "scenario": [
            "all_extension_months",
            "without_worst_month",
        ],
        "avg_diff_bp": [
            avg_diff_bp_all,
            avg_diff_bp_wo,
        ],
        "rel_diff_pct": [
            avg_rel_all,
            avg_rel_wo,
        ],
        "worst_month_removed": [
            "",
            str(worst_row['date'].date()),
        ],
    })

    robustness_df.to_csv(
        OUTPUT_DIR / "figure1_extension_robustness.csv",
        index=False
    )
    print("\nSaved to output/figure1_extension/figure1_extension_robustness.csv")


def plot_figure1_extension(df_all):
    """
    Plot Figure 1 with extended sample period.
    Adds a vertical line to mark the original paper's end date.
    """
    fig, ax = plt.subplots(figsize=(14, 5))

    ax.plot(df_all['date'], df_all['r_1y'] * 100,
            color='blue', label='Option-implied rate')
    ax.plot(df_all['date'], df_all['zero_1y'] * 100,
            color='orange', linestyle='--', label='Zero curve rate')
    ax.plot(df_all['date'], df_all['treasury_1y'] * 100,
            color='gold', linestyle='-.', label='Constant maturity Treasury rate')

    # Mark original sample end
    ax.axvline(
        x=pd.Timestamp(PAPER_END),
        color='red',
        linestyle='--',
        alpha=0.5,
        label='Original sample end (Dec 2022)'
    )

    ax.set_ylabel('Annual interest rate in %')
    ax.set_xlabel('Date')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        OUTPUT_DIR / "figure1_extension.png",
        dpi=150,
        bbox_inches='tight'
    )
    plt.show()
    print("\nSaved to output/figure1_extension.png")


if __name__ == "__main__":
    print("=" * 60)
    print("PLOTTING FIGURE 1 EXTENSION")
    print("=" * 60)

    df_all = load_data()
    print(f"\nData loaded: {len(df_all)} rows")
    print(f"Date range: {df_all['date'].min()} to {df_all['date'].max()}")

    print_summary(df_all)
    save_extension_diagnostics(df_all)
    save_extension_robustness_check(df_all)
    plot_figure1_extension(df_all)

    # Save series data
    df_all.to_csv(
        OUTPUT_DIR / "figure1_extension_series.csv", index=False
    )
    print("\nSaved to output/figure1_extension_series.csv")