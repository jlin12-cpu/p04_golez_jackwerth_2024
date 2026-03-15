"""
dodo.py — PyDoit task definitions for Golez & Jackwerth (2024) replication.

Run the full pipeline:
    doit

Run a specific task:
    doit <task_name>

List all tasks:
    doit list

The pipeline is organized into four layers:
    1. Pull   : fetch raw data from WRDS / FRED / Kenneth French
    2. Clean  : standardize and tidy raw data
    3. Calc   : compute implied rates, strip prices, and returns
    4. Output : produce figures and tables
"""

from pathlib import Path

# =============================================================================
# Paths
# =============================================================================

SRC = Path("src")
DATA = Path("_data")
CALC = DATA / "calc"
OUT = Path("output")


# =============================================================================
# Helpers
# =============================================================================

def task_config():
    """Create required directories if they do not exist."""
    return {
        "actions": [
            "mkdir -p _data",
            "mkdir -p _data/calc",
            "mkdir -p output",
        ],
        "verbosity": 2,
    }


# =============================================================================
# Layer 1: Pull raw data
# =============================================================================

def task_pull_crsp_spindx():
    """Pull daily S&P 500 index data (spindx, vwretd, vwretx, sprtrn) from CRSP."""
    return {
        "actions": [f"python {SRC / 'pull_crsp_spindx_level.py'}"],
        "targets": [DATA / "crsp_sp500_daily.parquet"],
        "task_dep": ["config"],
        "verbosity": 2,
    }


def task_pull_crsp_treasuries():
    """Pull monthly 2-year and 10-year Treasury returns from CRSP."""
    return {
        "actions": [f"python {SRC / 'pull_crsp_treasuries.py'}"],
        "targets": [DATA / "crsp_treasury_returns.parquet"],
        "task_dep": ["config"],
        "verbosity": 2,
    }


def task_pull_fred():
    """Pull Treasury rates and Fama-French factors (daily + monthly)."""
    return {
        "actions": [f"python {SRC / 'pull_fred.py'}"],
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
        "actions": [f"python {SRC / 'pull_spx_options_and_zero_coupon.py'}"],
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
        "actions": [f"python {SRC / 'clean_data.py'}"],
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
        "actions": [f"python {SRC / 'calc_implied_rates.py'}"],
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
        "actions": [f"python {SRC / 'calc_strip_prices.py'}"],
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
        "actions": [f"python {SRC / 'calc_returns.py'}"],
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
        "actions": [f"python {SRC / 'plot_figure1.py'}"],
        "file_dep": [
            CALC / "implied_rates_1y.parquet",
            CALC / "zero_curve_1y.parquet",
            DATA / "clean_rates.parquet",
        ],
        "targets": [
            OUT / "figure1/figure1.png",
            OUT / "figure1/figure1_series.csv",
            OUT / "figure1/figure1_summary.csv",
            OUT / "figure1/figure1_summary.tex",
        ],
        "task_dep": ["calc_implied_rates"],
        "verbosity": 2,
    }


def task_plot_figure1_extension():
    """Extended Figure 1: 12-month interest rates beyond 2022."""
    return {
        "actions": [f"python {SRC / 'plot_figure1_extension.py'}"],
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
            OUT / "figure1_extension/figure1_extension_summary.tex",
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
        "actions": [f"python {SRC / 'figure2.py'}"],
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
        "actions": [f"python {SRC / 'figure2_extended.py'}"],
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
        "actions": [f"python {SRC / 'figure2_extended_winsorized.py'}"],
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
        "actions": [f"python {SRC / 'figure3.py'}"],
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
        "actions": [f"python {SRC / 'figure3_extended.py'}"],
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
        "actions": [f"python {SRC / 'table1.py'}"],
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
        "actions": [f"python {SRC / 'table1_extended.py'}"],
        "file_dep": [CALC / "monthly_returns.parquet"],
        "targets": [
            OUT / "table1_extended.csv",
            OUT / "table1_extended.tex",
        ],
        "task_dep": ["calc_returns"],
        "verbosity": 2,
    }


# =============================================================================
# Full pipeline
# =============================================================================

def task_all():
    """Run the full replication pipeline."""
    return {
        "actions": None,
        "task_dep": [
            "plot_figure1",
            "plot_figure1_extension",
            "figure2",
            "figure2_extended",
            "figure2_extended_winsorized",
            "figure3",
            "figure3_extended",
            "table1",
            "table1_extended",
        ],
    }