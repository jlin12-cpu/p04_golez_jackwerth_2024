"""
Generate LaTeX tables from CSV output files.

Reads summary CSV files produced by plot_*.py scripts
and converts them to .tex files for inclusion in the LaTeX report.

Inputs:
    output/figure1/figure1_summary.csv
    output/figure1_extension/figure1_extension_summary.csv

Outputs:
    output/figure1/figure1_summary.tex
    output/figure1_extension/figure1_extension_summary.tex
"""

from pathlib import Path
import pandas as pd

OUTPUT_DIR = Path("output")


def generate_figure1_summary_table():
    """
    Convert Figure 1 summary statistics to LaTeX table.
    """
    df = pd.read_csv(OUTPUT_DIR / "figure1/figure1_summary.csv")

    df = df.rename(columns={
        "statistic": "Statistic",
        "value": "Our Value",
        "paper_value": "Paper Value",
    })

    df["Statistic"] = df["Statistic"].replace({
        "avg_diff_bp": "Average difference (bp)",
        "rel_full_pct": "Full-sample relative difference (\\%)",
        "rel_first_half_pct": "First-half relative difference (\\%)",
        "rel_second_half_pct": "Second-half relative difference (\\%)",
    })

    df["Our Value"] = df["Our Value"].apply(lambda x: f"{x:.2f}")
    df["Paper Value"] = df["Paper Value"].apply(lambda x: f"{x:.2f}")

    latex = df.to_latex(
        index=False,
        escape=False,
        column_format="lrr",
    )

    out_path = OUTPUT_DIR / "figure1/figure1_summary.tex"
    with open(out_path, "w") as f:
        f.write(latex)

    print(f"Saved {out_path}")


def generate_figure1_extension_summary_table():
    """
    Convert Figure 1 extension summary statistics to LaTeX table.
    """
    df = pd.read_csv(
        OUTPUT_DIR / "figure1_extension/figure1_extension_summary.csv"
    )

    df = df.rename(columns={
        "period": "Period",
        "avg_diff_bp": "Avg Diff (bp)",
        "rel_diff_pct": "Rel Diff (\\%)",
        "paper_avg_diff_bp": "Paper Avg Diff (bp)",
        "paper_rel_diff_pct": "Paper Rel Diff (\\%)",
    })

    df["Period"] = df["Period"].replace({
        "original_1996_2022": "Original sample (1996--2022)",
        "extended_2023_2024": "Extended period (2023--2024)",
        "full_1996_2024": "Full sample (1996--2024)",
    })

    for col in ["Avg Diff (bp)", "Rel Diff (\\%)"]:
        df[col] = df[col].apply(lambda x: f"{x:.2f}")

    for col in ["Paper Avg Diff (bp)", "Paper Rel Diff (\\%)"]:
        df[col] = df[col].apply(
            lambda x: f"{x:.2f}" if pd.notna(x) else "--"
        )

    latex = df.to_latex(
        index=False,
        escape=False,
        column_format="lrrrr",
    )

    out_path = OUTPUT_DIR / "figure1_extension/figure1_extension_summary.tex"
    with open(out_path, "w") as f:
        f.write(latex)

    print(f"Saved {out_path}")

if __name__ == "__main__":
    print("=" * 60)
    print("GENERATING LATEX TABLES")
    print("=" * 60)

    print("\n1. Figure 1 summary table...")
    generate_figure1_summary_table()

    print("\n2. Figure 1 extension summary table...")
    generate_figure1_extension_summary_table()

    print("\nDone!")