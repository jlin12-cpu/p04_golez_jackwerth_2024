"""
Replicate Figure 1 from Golez & Jackwerth (2024).

Figure 1: 12-month maturity interest rates (1996-2022)
- Option-implied rate
- Zero curve rate
- Constant maturity Treasury rate

Inputs:
    _data/calc/implied_rates_1y.parquet
    _data/calc/zero_curve_1y.parquet
    _data/clean_rates.parquet

Outputs:
    output/figure1.png
    output/figure1_series.csv
    output/figure1_summary.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DATA_DIR = Path("_data")
CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output/figure1")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Sample period (paper)
START_DATE = "1996-01-01"
END_DATE = "2022-12-31"


def load_data():
    """
    Load and merge all three interest rate series.
    """
    # Implied rate
    df_imp = pd.read_parquet(CALC_DIR / "implied_rates_1y.parquet")

    # Zero curve
    df_zero = pd.read_parquet(CALC_DIR / "zero_curve_1y.parquet")

    # Treasury rate
    df_treas = pd.read_parquet(DATA_DIR / "clean_rates.parquet")
    df_treas = df_treas[['date', 'treasury_1y']].dropna()

    # Merge
    df_all = (
        df_imp
        .merge(df_zero, on='date', how='inner')
        .merge(df_treas, on='date', how='inner')
        .sort_values('date')
    )

    # Filter to paper sample period
    df_all = df_all[
        (df_all['date'] >= START_DATE) &
        (df_all['date'] <= END_DATE)
    ].reset_index(drop=True)

    return df_all


def print_summary(df_all):
    """
    Print and save summary statistics to verify against paper.
    """
    diff_bp = (df_all['r_1y'] - df_all['zero_1y']) * 10000
    rel_full = ((df_all['r_1y'] - df_all['zero_1y']).mean() /
                df_all['zero_1y'].mean()) * 100

    first_half = df_all[df_all['date'] <= '2009-12-31']
    second_half = df_all[df_all['date'] >= '2010-01-01']

    rel_first = ((first_half['r_1y'] - first_half['zero_1y']).mean() /
                 first_half['zero_1y'].mean()) * 100
    rel_second = ((second_half['r_1y'] - second_half['zero_1y']).mean() /
                  second_half['zero_1y'].mean()) * 100

    print("\nSummary Statistics:")
    print(f"  Average difference (implied - zero curve): {diff_bp.mean():.2f} bp")
    print("  [Paper: 7 bp]")
    print(f"  Full-sample relative difference:  {rel_full:.2f}%")
    print("  [Paper: 2.82%]")
    print(f"  First half relative difference:   {rel_first:.2f}%")
    print("  [Paper: 0.64%]")
    print(f"  Second half relative difference:  {rel_second:.2f}%")
    print("  [Paper: 11.80%]")

    summary_df = pd.DataFrame({
        "statistic": [
            "avg_diff_bp",
            "rel_full_pct",
            "rel_first_half_pct",
            "rel_second_half_pct",
        ],
        "value": [
            diff_bp.mean(),
            rel_full,
            rel_first,
            rel_second,
        ],
        "paper_value": [
            7.00,
            2.82,
            0.64,
            11.80,
        ],
    })

    summary_df.to_csv(OUTPUT_DIR / "figure1_summary.csv", index=False)
    print("\nSaved to output/figure1_summary.csv")


def plot_figure1(df_all):
    """
    Plot Figure 1: 12-month interest rates.
    """
    fig, ax = plt.subplots(figsize=(12, 5))

    ax.plot(df_all['date'], df_all['r_1y'] * 100,
            color='blue', label='Option-implied rate')
    ax.plot(df_all['date'], df_all['zero_1y'] * 100,
            color='orange', linestyle='--', label='Zero curve rate')
    ax.plot(df_all['date'], df_all['treasury_1y'] * 100,
            color='gold', linestyle='-.', label='Constant maturity Treasury rate')

    ax.set_ylabel('Annual interest rate in %')
    ax.set_xlabel('Date')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "figure1.png", dpi=150, bbox_inches='tight')
    plt.show()
    print(f"\nSaved to output/figure1.png")


if __name__ == "__main__":
    print("=" * 60)
    print("PLOTTING FIGURE 1")
    print("=" * 60)

    df_all = load_data()
    print(f"\nData loaded: {len(df_all)} rows")
    print(f"Date range: {df_all['date'].min()} to {df_all['date'].max()}")

    print_summary(df_all)
    plot_figure1(df_all)

    # Save series data
    df_all.to_csv(OUTPUT_DIR / "figure1_series.csv", index=False)
    print("\nSaved to output/figure1_series.csv")