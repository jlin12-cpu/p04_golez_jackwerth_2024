"""
dodo.py — PyDoit task definitions for Golez & Jackwerth (2024) replication.

Run the full pipeline:
    doit
    doit all

Run a specific task:
    doit <task_name>

List all tasks:
    doit list

The pipeline is organized into six layers:
    1. Pull      : fetch raw data from WRDS / FRED / Kenneth French
    2. Clean     : standardize and tidy raw data
    3. Calc      : compute implied rates, strip prices, and returns
    4. Output    : produce figures and tables
    5. Report    : generate LaTeX tables, run tests, compile PDF
    6. Website   : build chartbook site (kept separate until fully stabilized)
"""

from pathlib import Path
import sys

# =============================================================================
# Paths
# =============================================================================

SRC = Path("src")
DATA = Path("_data")
CALC = DATA / "calc"
OUT = Path("output")
REPORTS = Path("reports")
PYTHON = sys.executable


# =============================================================================
# Helpers
# =============================================================================

def _make_dirs():
    """Create required project directories (cross-platform, no shell needed)."""
    Path("_data").mkdir(parents=True, exist_ok=True)
    Path("_data/calc").mkdir(parents=True, exist_ok=True)
    Path("output").mkdir(parents=True, exist_ok=True)


def task_config():
    """Create required directories if they do not exist."""
    return {
        "actions": [_make_dirs],
        "verbosity": 2,
    }


# =============================================================================
# Layer 1: Pull raw data
# =============================================================================

def task_pull_crsp_spindx():
    """Pull daily S&P 500 index data (spindx, vwretd, vwretx, sprtrn) from CRSP."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "pull_crsp_spindx_level.py"}'],
        "targets": [DATA / "crsp_sp500_daily.parquet"],
        "task_dep": ["config"],
        "verbosity": 2,
    }


def task_pull_crsp_treasuries():
    """Pull monthly 2-year and 10-year Treasury returns from CRSP."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "pull_crsp_treasuries.py"}'],
        "targets": [DATA / "crsp_treasury_returns.parquet"],
        "task_dep": ["config"],
        "verbosity": 2,
    }


def task_pull_fred():
    """Pull Treasury rates and Fama-French factors (daily + monthly)."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "pull_fred.py"}'],
        "targets": [
            DATA / "fred_treasury_rates.parquet",
            DATA / "fama_french_factors.parquet",
            DATA / "fama_french_monthly.parquet",
        ],
        "task_dep": ["config"],
        "verbosity": 2,
    }


def task_pull_optionmetrics():
    """Pull SPX options (month-end) and zero curve from OptionMetrics via WRDS."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "pull_spx_options_and_zero_coupon.py"}'],
        "targets": [
            DATA / "optionmetrics_spx_raw.parquet",
            DATA / "optionmetrics_spx_monthly.parquet",
            DATA / "optionmetrics_zero_curve.parquet",
        ],
        "task_dep": ["config"],
        "verbosity": 2,
    }


# =============================================================================
# Layer 2: Clean data
# =============================================================================

def task_clean_data():
    """Standardize and tidy raw data into analysis-ready parquet files."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "clean_data.py"}'],
        "file_dep": [
            DATA / "optionmetrics_spx_monthly.parquet",
            DATA / "optionmetrics_zero_curve.parquet",
            DATA / "fred_treasury_rates.parquet",
            DATA / "crsp_sp500_daily.parquet",
        ],
        "targets": [
            DATA / "clean_options.parquet",
            DATA / "clean_zero_curve.parquet",
            DATA / "clean_rates.parquet",
            DATA / "clean_crsp_sp500_monthly.parquet",
        ],
        "task_dep": [
            "config",
            "pull_crsp_spindx",
            "pull_fred",
            "pull_optionmetrics",
        ],
        "verbosity": 2,
    }


# =============================================================================
# Layer 3: Calc
# =============================================================================

def task_calc_implied_rates():
    """Calculate option-implied interest rates and interpolate to 1-year maturity."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "calc_implied_rates.py"}'],
        "file_dep": [
            DATA / "clean_options.parquet",
            DATA / "clean_zero_curve.parquet",
        ],
        "targets": [
            CALC / "implied_rates.parquet",
            CALC / "implied_rates_1y.parquet",
            CALC / "zero_curve_1y.parquet",
        ],
        "task_dep": ["clean_data"],
        "verbosity": 2,
    }


def task_calc_strip_prices():
    """Calculate dividend strip prices from SPX options via put-call parity."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "calc_strip_prices.py"}'],
        "file_dep": [
            DATA / "clean_options.parquet",
            CALC / "implied_rates.parquet",
        ],
        "targets": [
            CALC / "strip_prices.parquet",
            CALC / "all_strip_prices.parquet",
        ],
        "task_dep": ["calc_implied_rates"],
        "verbosity": 2,
    }


def task_calc_returns():
    """Calculate monthly strip and market returns plus excess-return variants."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "calc_returns.py"}'],
        "file_dep": [
            CALC / "strip_prices.parquet",
            CALC / "all_strip_prices.parquet",
            DATA / "clean_crsp_sp500_monthly.parquet",
            DATA / "fama_french_monthly.parquet",
            DATA / "crsp_treasury_returns.parquet",
        ],
        "targets": [
            CALC / "monthly_returns.parquet",
        ],
        "task_dep": [
            "calc_strip_prices",
            "pull_crsp_treasuries",
            "pull_fred",
        ],
        "verbosity": 2,
    }


# =============================================================================
# Layer 4: Output — Figure 1
# =============================================================================

def task_plot_figure1():
    """Replicate Figure 1: 12-month interest rates (1996-2022)."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "plot_figure1.py"}'],
        "file_dep": [
            CALC / "implied_rates_1y.parquet",
            CALC / "zero_curve_1y.parquet",
            DATA / "clean_rates.parquet",
        ],
        "targets": [
            OUT / "figure1/figure1.png",
            OUT / "figure1/figure1_series.csv",
            OUT / "figure1/figure1_summary.csv",
        ],
        "task_dep": ["calc_implied_rates"],
        "verbosity": 2,
    }


def task_plot_figure1_summary_stats():
    """Generate Figure 1 descriptive statistics table and implied-zero spread chart."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "plot_figure1_summary_stats.py"}'],
        "file_dep": [
            OUT / "figure1/figure1_series.csv",
        ],
        "targets": [
            OUT / "figure1_summary_stats/figure1_summary_stats_table.csv",
            OUT / "figure1_summary_stats/figure1_summary_stats_table.tex",
            OUT / "figure1_summary_stats/figure1_implied_zero_spread.png",
        ],
        "task_dep": ["plot_figure1"],
        "verbosity": 2,
    }


def task_plot_figure1_extension():
    """Extended Figure 1: 12-month interest rates beyond 2022."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "plot_figure1_extension.py"}'],
        "file_dep": [
            CALC / "implied_rates_1y.parquet",
            CALC / "zero_curve_1y.parquet",
            DATA / "clean_rates.parquet",
        ],
        "targets": [
            OUT / "figure1_extension/figure1_extension.png",
            OUT / "figure1_extension/figure1_extension_diagnostics.csv",
            OUT / "figure1_extension/figure1_extension_robustness.csv",
            OUT / "figure1_extension/figure1_extension_series.csv",
            OUT / "figure1_extension/figure1_extension_summary.csv",
        ],
        "task_dep": ["calc_implied_rates"],
        "verbosity": 2,
    }


# =============================================================================
# Layer 4: Output — Figure 2
# =============================================================================

def task_figure2():
    """Replicate Figure 2: cumulative returns (1996-2022)."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "figure2.py"}'],
        "file_dep": [CALC / "monthly_returns.parquet"],
        "targets": [
            OUT / "figure2/figure2.png",
            OUT / "figure2/figure2_series.csv",
            OUT / "figure2/figure2_terminal_comparison.csv",
        ],
        "task_dep": ["calc_returns"],
        "verbosity": 2,
    }


def task_figure2_extended():
    """Extended Figure 2: cumulative returns beyond 2022."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "figure2_extended.py"}'],
        "file_dep": [CALC / "monthly_returns.parquet"],
        "targets": [
            OUT / "figure2_extended/figure2_extended.png",
            OUT / "figure2_extended/figure2_extended_series.csv",
            OUT / "figure2_extended/figure2_extended_terminal.csv",
        ],
        "task_dep": ["calc_returns"],
        "verbosity": 2,
    }


def task_figure2_extended_winsorized():
    """Extended Figure 2 with winsorized strip returns (robustness check)."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "figure2_extended_winsorized.py"}'],
        "file_dep": [CALC / "monthly_returns.parquet"],
        "targets": [
            OUT / "figure2_extended_winsorized/figure2_extended_winsorized.png",
            OUT / "figure2_extended_winsorized/figure2_extended_winsorized_series.csv",
            OUT / "figure2_extended_winsorized/figure2_extended_winsorized_terminal.csv",
        ],
        "task_dep": ["calc_returns"],
        "verbosity": 2,
    }


# =============================================================================
# Layer 4: Output — Figure 3
# =============================================================================

def task_figure3():
    """Replicate Figure 3: annualized std across holding periods (1996-2022)."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "figure3.py"}'],
        "file_dep": [CALC / "monthly_returns.parquet"],
        "targets": [
            OUT / "figure3/figure3.png",
            OUT / "figure3/figure3_series.csv",
        ],
        "task_dep": ["calc_returns"],
        "verbosity": 2,
    }


def task_figure3_extended():
    """Extended Figure 3: annualized std across holding periods beyond 2022."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "figure3_extended.py"}'],
        "file_dep": [CALC / "monthly_returns.parquet"],
        "targets": [
            OUT / "figure3_extended/figure3_extended.png",
            OUT / "figure3_extended/figure3_extended_series.csv",
        ],
        "task_dep": ["calc_returns"],
        "verbosity": 2,
    }


# =============================================================================
# Layer 4: Output — Table 1
# =============================================================================

def task_table1():
    """Replicate Table 1: monthly return summary statistics (1996-2022)."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "table1.py"}'],
        "file_dep": [CALC / "monthly_returns.parquet"],
        "targets": [
            OUT / "table1.csv",
            OUT / "table1.tex",
        ],
        "task_dep": ["calc_returns"],
        "verbosity": 2,
    }


def task_table1_extended():
    """Extended Table 1: monthly return summary statistics beyond 2022."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "table1_extended.py"}'],
        "file_dep": [CALC / "monthly_returns.parquet"],
        "targets": [
            OUT / "table1_extended.csv",
            OUT / "table1_extended.tex",
        ],
        "task_dep": ["calc_returns"],
        "verbosity": 2,
    }


# =============================================================================
# Layer 5: Report — generate LaTeX tables
# =============================================================================

def task_generate_latex_tables():
    """
    Convert summary CSV files to LaTeX .tex files for input{} in report.
    """
    return {
        "actions": [f'"{PYTHON}" {SRC / "generate_latex_tables.py"}'],
        "file_dep": [
            OUT / "figure1/figure1_summary.csv",
            OUT / "figure1_extension/figure1_extension_summary.csv",
        ],
        "targets": [
            OUT / "figure1/figure1_summary.tex",
            OUT / "figure1_extension/figure1_extension_summary.tex",
        ],
        "task_dep": [
            "plot_figure1",
            "plot_figure1_extension",
        ],
        "verbosity": 2,
    }


# =============================================================================
# Layer 5: Report — run tests
# =============================================================================

def task_run_tests():
    """
    Run all project unit tests.
    """
    return {
        "actions": [
            "pytest src/test_clean_data.py src/test_figure1.py src/test_table1.py src/test_figure2.py src/test_figure3.py -v --tb=short"
        ],
        "task_dep": [
            "clean_data",
            "calc_returns",
            "plot_figure1",
            "plot_figure1_summary_stats",
            "figure2",
            "figure3",
            "table1",
            "generate_latex_tables",
        ],
        "verbosity": 2,
    }


# =============================================================================
# Layer 5: Report — compile PDF
# =============================================================================
def task_own_analysis():
    """Generate own analysis figures and summary statistics table for the report."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "own_analysis.py"}'],
        "file_dep": [
            DATA / "crsp_sp500_daily.parquet",
            DATA / "optionmetrics_spx_monthly.parquet",
            DATA / "optionmetrics_zero_curve.parquet",
            DATA / "fama_french_monthly.parquet",
            DATA / "crsp_treasury_returns.parquet",
        ],
        "targets": [
            OUT / "data_summary.tex",
            OUT / "figure_A_input_series.png",
            OUT / "figure_B_options_coverage.png",
        ],
        "task_dep": [
            "pull_crsp_spindx",
            "pull_crsp_treasuries",
            "pull_fred",
            "pull_optionmetrics",
        ],
        "verbosity": 2,
    }

def task_compile_report():
    """
    Compile reports/report.tex to PDF using latexmk.

    Uses latexmk -cd so that relative paths (PathToOutput = ../output)
    resolve correctly on both macOS/Linux and Windows without needing
    a shell cd command.
    """
    return {
        "actions": [
            "latexmk -pdf -cd -interaction=nonstopmode reports/report.tex",
        ],
        "file_dep": [
            REPORTS / "report.tex",
            REPORTS / "bibliography.bib",
            OUT / "figure1/figure1_summary.tex",
            OUT / "figure1_extension/figure1_extension_summary.tex",
            OUT / "figure1_summary_stats/figure1_summary_stats_table.tex",
            OUT / "table1.tex",
            OUT / "table1_extended.tex",
            OUT / "figure1/figure1.png",
            OUT / "figure1_summary_stats/figure1_implied_zero_spread.png",
            OUT / "figure1_extension/figure1_extension.png",
            OUT / "figure2/figure2.png",
            OUT / "figure2_extended_winsorized/figure2_extended_winsorized.png",
            OUT / "figure3/figure3.png",
            OUT / "figure3_extended/figure3_extended.png",
            OUT / "data_summary.tex",
            OUT / "figure_A_input_series.png",
            OUT / "figure_B_options_coverage.png",
        ],
        "targets": [
            REPORTS / "report.pdf",
        ],
        "task_dep": [
            "plot_figure1",
            "plot_figure1_summary_stats",
            "plot_figure1_extension",
            "figure2",
            "figure2_extended_winsorized",
            "figure3",
            "figure3_extended",
            "table1",
            "table1_extended",
            "generate_latex_tables",
            "own_analysis"
        ],
        "verbosity": 2,
    }


# =============================================================================
# Layer 6: Website — generate HTML pages for chartbook
# =============================================================================

def task_generate_chartbook_html():
    """Generate self-contained HTML pages for figures and tables for the chartbook website."""
    return {
        "actions": [f'"{PYTHON}" {SRC / "generate_chartbook_html.py"}'],
        "file_dep": [
            OUT / "figure1/figure1.png",
            OUT / "figure1/figure1_summary.csv",
            OUT / "figure1_extension/figure1_extension.png",
            OUT / "figure1_extension/figure1_extension_summary.csv",
            OUT / "figure1_summary_stats/figure1_implied_zero_spread.png",
            OUT / "figure1_summary_stats/figure1_summary_stats_table.csv",
            OUT / "figure2/figure2.png",
            OUT / "figure2/figure2_terminal_comparison.csv",
            OUT / "figure2_extended/figure2_extended.png",
            OUT / "figure2_extended/figure2_extended_terminal.csv",
            OUT / "figure2_extended_winsorized/figure2_extended_winsorized.png",
            OUT / "figure2_extended_winsorized/figure2_extended_winsorized_terminal.csv",
            OUT / "figure3/figure3.png",
            OUT / "figure3/figure3_series.csv",
            OUT / "figure3_extended/figure3_extended.png",
            OUT / "figure3_extended/figure3_extended_series.csv",
            OUT / "table1.csv",
            OUT / "table1_extended.csv",
        ],
        "targets": [
            OUT / "site_html/figure1.html",
            OUT / "site_html/figure1_extension.html",
            OUT / "site_html/figure1_summary_stats.html",
            OUT / "site_html/figure2.html",
            OUT / "site_html/figure2_extension.html",
            OUT / "site_html/figure2_extension_winsorized.html",
            OUT / "site_html/figure3.html",
            OUT / "site_html/figure3_extension.html",
            OUT / "site_html/table1.html",
            OUT / "site_html/table1_extended.html",
        ],
        "task_dep": [
            "plot_figure1",
            "plot_figure1_summary_stats",
            "plot_figure1_extension",
            "figure2",
            "figure2_extended",
            "figure2_extended_winsorized",
            "figure3",
            "figure3_extended",
            "table1",
            "table1_extended",
        ],
        "verbosity": 2,
    }


def task_build_chartbook_site():
    """
    Build the chartbook website into docs/.

    This task is kept separate from task_all() so that the main replication
    pipeline remains stable even if website styling or chartbook rendering
    needs additional debugging.

    Run explicitly with:
        doit build_chartbook_site
    """
    return {
        "actions": [
            "chartbook build -f",
        ],
        "file_dep": [
            "README.md",
            "chartbook.toml",
        ],
        "targets": [
            "docs/index.html",
        ],
        "task_dep": ["generate_chartbook_html"],  # 确保HTML先生成
        "verbosity": 2,
    }

# =============================================================================
# Full pipeline
# =============================================================================

def task_all():
    """Run the full replication pipeline including tests and report compilation."""
    return {
        "actions": None,
        "task_dep": [
            "plot_figure1",
            "plot_figure1_summary_stats",
            "plot_figure1_extension",
            "figure2",
            "figure2_extended",
            "figure2_extended_winsorized",
            "figure3",
            "figure3_extended",
            "table1",
            "table1_extended",
            "generate_latex_tables",
            "run_tests",
            "compile_report",
            "build_chartbook_site", 
        ],
    }