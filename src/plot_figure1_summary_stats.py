"""
Generate summary statistics and charts for Figure 1 data.

Produces one table and one chart providing additional context
for the three interest rate series used in Figure 1.

Table: Descriptive statistics for each rate series
       (full sample, first half, second half)

Chart: Time series of the implied-minus-zero spread

Inputs:
    output/figure1/figure1_series.csv

Outputs:
    output/figure1_summary_stats/figure1_summary_stats_table.csv
    output/figure1_summary_stats/figure1_summary_stats_table.tex
    output/figure1_summary_stats/figure1_implied_zero_spread.png
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

OUTPUT_DIR = Path("output/figure1_summary_stats")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
FIGURE1_DIR = Path("output/figure1")

FIRST_HALF_END = "2009-12-31"
SECOND_HALF_START = "2010-01-01"


def load_data():
    """
    Load the merged Figure 1 dataset.
    """
    return pd.read_csv(
        FIGURE1_DIR / "figure1_series.csv",
        parse_dates=["date"]
    )


def compute_descriptive_stats(df):
    """
    Compute descriptive statistics for each rate series
    across the full sample, first half, and second half.
    """
    def stats_for(data, col):
        return {
            "Mean (%)": data[col].mean() * 100,
            "Std. Dev. (%)": data[col].std() * 100,
            "Min (%)": data[col].min() * 100,
            "Max (%)": data[col].max() * 100,
            "N": len(data),
        }

    periods = {
        "Full sample (1996--2022)": df,
        "First half (1996--2009)": df[df["date"] <= FIRST_HALF_END],
        "Second half (2010--2022)": df[df["date"] >= SECOND_HALF_START],
    }

    cols = {
        "Option-implied rate": "r_1y",
        "Zero curve rate": "zero_1y",
        "Treasury rate": "treasury_1y",
    }

    rows = []
    for period_name, period_df in periods.items():
        for col_name, col in cols.items():
            s = stats_for(period_df, col)
            rows.append({
                "Period": period_name,
                "Series": col_name,
                **s,
            })

    return pd.DataFrame(rows)


def save_summary_table(df_stats):
    """
    Save descriptive statistics as both CSV and LaTeX.
    """
    df_stats.to_csv(
        OUTPUT_DIR / "figure1_summary_stats_table.csv",
        index=False
    )

    df_out = df_stats.copy()
    for col in ["Mean (%)", "Std. Dev. (%)", "Min (%)", "Max (%)"]:
        df_out[col] = df_out[col].map(lambda x: f"{x:.2f}")
    df_out["N"] = df_out["N"].astype(int)

    # Rename columns to escape % for LaTeX
    df_out = df_out.rename(columns={
        "Mean (%)":     r"Mean (\%)",
        "Std. Dev. (%)": r"Std. Dev. (\%)",
        "Min (%)":      r"Min (\%)",
        "Max (%)":      r"Max (\%)",
    })

    latex = df_out.to_latex(
        index=False,
        escape=False,
        column_format="llrrrrr",
    )

    out_path = OUTPUT_DIR / "figure1_summary_stats_table.tex"
    with open(out_path, "w") as f:
        f.write(latex)

    print(f"Saved {OUTPUT_DIR / 'figure1_summary_stats_table.csv'}")
    print(f"Saved {out_path}")


def plot_implied_zero_spread(df):
    """
    Plot the implied-minus-zero spread over time.
    """
    df = df.copy()
    df["spread_bp"] = (df["r_1y"] - df["zero_1y"]) * 10000

    fig, ax = plt.subplots(figsize=(12, 4))

    ax.plot(
        df["date"],
        df["spread_bp"],
        label="Implied minus zero curve (bp)"
    )
    ax.axhline(
        y=0,
        color="black",
        linestyle="--",
        alpha=0.5,
        label="Zero line"
    )
    ax.axhline(
        y=df["spread_bp"].mean(),
        color="red",
        linestyle="--",
        alpha=0.7,
        label=f"Sample mean ({df['spread_bp'].mean():.1f} bp)"
    )
    ax.axvline(
        x=pd.Timestamp(SECOND_HALF_START),
        color="gray",
        linestyle="--",
        alpha=0.7,
        label="Second half begins (2010)"
    )

    ax.set_ylabel("Spread (basis points)")
    ax.set_xlabel("Date")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=9)

    plt.tight_layout()
    plt.savefig(
        OUTPUT_DIR / "figure1_implied_zero_spread.png",
        dpi=150,
        bbox_inches="tight"
    )
    plt.close()
    print(f"Saved {OUTPUT_DIR / 'figure1_implied_zero_spread.png'}")


if __name__ == "__main__":
    print("=" * 60)
    print("GENERATING FIGURE 1 SUMMARY STATISTICS")
    print("=" * 60)

    df = load_data()
    print(f"\nLoaded {len(df)} rows")

    df_stats = compute_descriptive_stats(df)
    save_summary_table(df_stats)

    plot_implied_zero_spread(df)

    print("\nDone!")