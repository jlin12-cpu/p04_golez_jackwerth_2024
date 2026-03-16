"""
Generate own analysis figures and summary statistics table for the replication report.

Produces three output files used in the Data Summary section of report.tex:
    - output/data_summary.tex               : Summary statistics for all input data series
    - output/figure_A_input_series.png      : SPX index, zero curve, and options count
    - output/figure_B_options_coverage.png  : Options maturity and moneyness distributions

Inputs:
    _data/crsp_sp500_daily.parquet
    _data/optionmetrics_spx_monthly.parquet
    _data/optionmetrics_zero_curve.parquet
    _data/fama_french_monthly.parquet
    _data/crsp_treasury_returns.parquet
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DATA_DIR   = Path("_data")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_data():
    """Load all input data files."""
    df_spx   = pd.read_parquet(DATA_DIR / "crsp_sp500_daily.parquet")
    df_opt   = pd.read_parquet(DATA_DIR / "optionmetrics_spx_monthly.parquet")
    df_zero  = pd.read_parquet(DATA_DIR / "optionmetrics_zero_curve.parquet")
    df_ff    = pd.read_parquet(DATA_DIR / "fama_french_monthly.parquet")
    df_treas = pd.read_parquet(DATA_DIR / "crsp_treasury_returns.parquet")
    return df_spx, df_opt, df_zero, df_ff, df_treas


def generate_data_summary(df_spx, df_opt, df_zero, df_ff, df_treas):
    """
    Compute summary statistics for all input data series and save to LaTeX.

    Outputs:
        output/data_summary.tex
    """
    def summarize(df, col, label):
        s = df[col].dropna()
        return {
            'Series':  label,
            'N':       len(s),
            'Mean':    s.mean(),
            'Std':     s.std(),
            'Min':     s.min(),
            'Median':  s.median(),
            'Max':     s.max(),
            'Start':   df['date'].min().strftime('%Y-%m'),
            'End':     df['date'].max().strftime('%Y-%m'),
        }

    rows = [
        summarize(df_spx,   'spindx',          'SPX index level'),
        summarize(df_spx,   'vwretd',          'Market return (daily, incl. div.)'),
        summarize(df_opt,   'mid_price',        'SPX option mid price ($)'),
        summarize(df_opt,   'days_to_maturity', 'Days to maturity'),
        summarize(df_opt,   'impl_volatility',  'Implied volatility'),
        summarize(df_zero,  'rate',             'Zero curve rate (%)'),
        summarize(df_ff,    'rf_1m_monthly',    '1-month T-bill rate (monthly)'),
        summarize(df_treas, 'treasury_2y_ret',  '2Y Treasury return (monthly)'),
        summarize(df_treas, 'treasury_10y_ret', '10Y Treasury return (monthly)'),
    ]

    df_summary = pd.DataFrame(rows).set_index('Series')
    df_summary[['Mean', 'Std', 'Min', 'Median', 'Max']] = \
        df_summary[['Mean', 'Std', 'Min', 'Median', 'Max']].round(4)

    latex = df_summary.to_latex(escape=True, na_rep="")
    with open(OUTPUT_DIR / "data_summary.tex", "w") as f:
        f.write(latex)

    print("Saved output/data_summary.tex")
    return df_summary


def plot_figure_a(df_spx, df_zero, df_ff):
    """
    Plot Figure A: time series of key input variables.

    Panel A: S&P 500 index level (1996-2024)
    Panel B: 1-year zero curve rate vs. annualised 1-month T-bill rate
    Panel C: number of SPX option contracts per month-end date

    Outputs:
        output/figure_A_input_series.png
    """
    fig, axes = plt.subplots(3, 1, figsize=(10, 10))
    fig.suptitle("Figure A – Key Input Variables (1996–2024)", fontsize=13)

    # Panel A: SPX index level
    axes[0].plot(df_spx['date'], df_spx['spindx'], lw=1, color='steelblue')
    axes[0].set_title("A. S&P 500 Index Level")
    axes[0].set_ylabel("Index level")
    axes[0].set_xlabel("")

    # Panel B: zero curve rate vs T-bill
    zero_1y = df_zero[df_zero['days'].between(350, 380)].copy()
    zero_1y = zero_1y.groupby('date')['rate'].mean().reset_index()
    zero_1y['rate'] = zero_1y['rate'] / 100

    axes[1].plot(zero_1y['date'],  zero_1y['rate'],
                 lw=1.2, label='1Y zero curve rate', color='steelblue')
    axes[1].plot(df_ff['date'],    df_ff['rf_1m_monthly'] * 12,
                 lw=1.2, linestyle='--',
                 label='1M T-bill (annualised)', color='tomato')
    axes[1].set_title("B. 1-Year Zero Curve Rate vs. 1-Month T-Bill Rate")
    axes[1].set_ylabel("Rate")
    axes[1].legend(fontsize=9)

    # Panel C: number of options contracts per month
    opt_count = pd.read_parquet(
        DATA_DIR / "optionmetrics_spx_monthly.parquet"
    ).groupby('date').size().reset_index(name='n_contracts')
    axes[2].bar(opt_count['date'], opt_count['n_contracts'],
                width=20, color='steelblue', alpha=0.7)
    axes[2].set_title("C. Number of SPX Option Contracts per Month-End Date")
    axes[2].set_ylabel("Number of contracts")
    axes[2].set_xlabel("Date")

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "figure_A_input_series.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("Saved output/figure_A_input_series.png")


def plot_figure_b(df_spx, df_opt):
    """
    Plot Figure B: SPX options data coverage after filtering.

    Panel A: distribution of days to maturity
    Panel B: distribution of moneyness (K/S)

    Outputs:
        output/figure_B_options_coverage.png
    """
    # Apply paper filters
    df_opt_clean = df_opt[
        (df_opt['mid_price'] >= 3) &
        (df_opt['days_to_maturity'] >= 90)
    ].copy()

    # Merge spindx for moneyness calculation
    spx_monthly = df_spx.copy()
    spx_monthly['year_month'] = spx_monthly['date'].dt.to_period('M')
    spx_eom = spx_monthly.groupby('year_month')['spindx'].last().reset_index()
    spx_eom['date'] = spx_eom['year_month'].dt.to_timestamp('M')

    df_opt_clean['year_month'] = df_opt_clean['date'].dt.to_period('M')
    df_opt_clean = df_opt_clean.merge(
        spx_eom[['year_month', 'spindx']], on='year_month', how='left'
    )
    df_opt_clean['moneyness'] = df_opt_clean['strike'] / df_opt_clean['spindx']
    df_opt_clean = df_opt_clean[df_opt_clean['moneyness'].between(0.5, 1.5)]

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    fig.suptitle("Figure B – SPX Options Data Coverage After Filtering", fontsize=12)

    # Panel A: days to maturity distribution
    axes[0].hist(df_opt_clean['days_to_maturity'], bins=50,
                 color='steelblue', alpha=0.8, edgecolor='white')
    axes[0].axvline(365, color='tomato',  lw=1.5, linestyle='--', label='1 year')
    axes[0].axvline(730, color='orange',  lw=1.5, linestyle='--', label='2 years')
    axes[0].set_title("A. Distribution of Days to Maturity")
    axes[0].set_xlabel("Days to maturity")
    axes[0].set_ylabel("Number of contracts")
    axes[0].legend(fontsize=9)

    # Panel B: moneyness distribution
    axes[1].hist(df_opt_clean['moneyness'], bins=60,
                 color='steelblue', alpha=0.8, edgecolor='white')
    axes[1].axvline(1.0, color='tomato', lw=1.5, linestyle='--', label='ATM (K/S = 1)')
    axes[1].set_title("B. Distribution of Moneyness (K / S)")
    axes[1].set_xlabel("Moneyness (strike / SPX level)")
    axes[1].set_ylabel("Number of contracts")
    axes[1].legend(fontsize=9)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "figure_B_options_coverage.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("Saved output/figure_B_options_coverage.png")


if __name__ == "__main__":
    print("=" * 60)
    print("GENERATING OWN ANALYSIS FIGURES AND TABLES")
    print("=" * 60)

    df_spx, df_opt, df_zero, df_ff, df_treas = load_data()

    generate_data_summary(df_spx, df_opt, df_zero, df_ff, df_treas)
    plot_figure_a(df_spx, df_zero, df_ff)
    plot_figure_b(df_spx, df_opt)

    print("\nDone!")
    print("Output files:")
    print("  output/data_summary.tex")
    print("  output/figure_A_input_series.png")
    print("  output/figure_B_options_coverage.png")