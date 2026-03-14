"""
Extended replication of Table 1 from Golez & Jackwerth (2024).

Uses updated data through the present to assess whether the paper's
findings hold in the extended sample period.

Extends the paper's sample period (1996-2022) through 2024 to examine:
- Whether strip Sharpe ratios remain similar to or higher than market
- Whether the negative AR(1) of strip returns persists
- How the 2022-2024 period (rate hikes, recovery) affects results

Inputs:
    _data/calc/monthly_returns.parquet

Outputs:
    _output/table1_extended.csv
    _output/table1_extended.tex
"""

from pathlib import Path
import pandas as pd
import numpy as np

from table1 import build_table1, format_table1, to_latex

CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Extended sample period
START_YEAR = 1996
END_YEAR   = 2024


if __name__ == "__main__":
    print("=" * 60)
    print("TABLE 1 EXTENDED: MONTHLY RETURNS (ANNUALIZED)")
    print(f"Sample period: {START_YEAR}-{END_YEAR}")
    print("=" * 60)

    # Load monthly returns
    print("\nLoading monthly returns...")
    df_returns = pd.read_parquet(CALC_DIR / "monthly_returns.parquet")

    # Filter to extended sample period
    mask = (
        (df_returns["date"].dt.year >= START_YEAR) &
        (df_returns["date"].dt.year <= END_YEAR)
    )
    df = df_returns.loc[mask].copy()
    print(f"  Sample period: {START_YEAR}-{END_YEAR}")
    print(f"  Rows in sample: {len(df)}")

    # Build numeric table
    print("\nBuilding Table 1 (extended)...")
    df_table = build_table1(df)

    print("\nRaw numeric table:")
    print(df_table.round(3).to_string())

    # Build formatted table
    df_fmt = format_table1(df_table)

    print("\n" + "=" * 60)
    print("TABLE 1 EXTENDED (formatted)")
    print("=" * 60)
    print(df_fmt.to_string())

    # Comparison: paper vs extended
    print("\n" + "=" * 60)
    print("COMPARISON: PAPER (1996-2022) vs EXTENDED (1996-2024)")
    print("=" * 60)

    # Load paper period for comparison
    mask_paper = (
        (df_returns["date"].dt.year >= START_YEAR) &
        (df_returns["date"].dt.year <= 2022)
    )
    df_paper_period = df_returns.loc[mask_paper].copy()
    df_table_paper = build_table1(df_paper_period)

    print("\nPaper period (1996-2022):")
    print(df_table_paper[["Mean", "Std. dev.", "Sharpe ratio", "AR(1)"]].round(2).to_string())

    print(f"\nExtended period (1996-{END_YEAR}):")
    print(df_table[["Mean", "Std. dev.", "Sharpe ratio", "AR(1)"]].round(2).to_string())

    # Save outputs
    print("\nSaving...")
    df_fmt.to_csv(OUTPUT_DIR / "table1_extended.csv")
    with open(OUTPUT_DIR / "table1_extended.tex", "w") as f:
        f.write(to_latex(df_fmt))

    print("  Saved to output/table1_extended.csv")
    print("  Saved to output/table1_extended.tex")