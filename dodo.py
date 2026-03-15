"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based.
"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
import sys

sys.path.insert(1, "./src/")

import shutil
from os import environ, getcwd, path
from pathlib import Path

from colorama import Fore, Style, init
from doit.reporter import ConsoleReporter
from settings import config

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False


class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = (
            Fore.GREEN
            + doit_mark
            + f" {path.basename(getcwd())}: "
            + task
            + Style.RESET_ALL
        )
        self.outstream.write(output)


if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}

init(autoreset=True)

BASE_DIR = config("BASE_DIR")
DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
OS_TYPE = config("OS_TYPE")

environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"

# calc scripts hard-code _data/calc/ as their output directory
CALC_DIR = DATA_DIR / "calc"


# fmt: off
def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"
def jupyter_to_md(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupytext --to markdown --output-dir={output_dir} {notebook_path}"
def jupyter_clear_output(notebook_path):
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
# fmt: on


def mv(from_path, to_path):
    from_path = Path(from_path)
    to_path = Path(to_path)
    to_path.mkdir(parents=True, exist_ok=True)
    if OS_TYPE == "nix":
        return f"mv {from_path} {to_path}"
    else:
        return f"move {from_path} {to_path}"


def copy_file(origin_path, destination_path, mkdir=True):
    def _copy_file():
        origin = Path(origin_path)
        dest = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)
    return _copy_file


##################################
## Begin PyDoit tasks
##################################


def task_config():
    """Create empty directories for data and output if they don't exist"""
    return {
        "actions": ["python ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


# ---------------------------------------------------------------------------
# task_pull: pull all raw data from WRDS / FRED
# ---------------------------------------------------------------------------

def task_pull():
    """Pull data from external sources"""

    # FRED: Treasury rates (rf_1m, treasury_2y, treasury_10y)
    #       + Fama-French factors (mkt_rf, smb, hml, rf_1m)
    yield {
        "name": "fred",
        "doc": "Pull Treasury rates and Fama-French factors from FRED",
        "actions": [
            "python ./src/settings.py",
            "python ./src/pull_fred.py",
        ],
        "targets": [
            DATA_DIR / "fred_treasury_rates.parquet",
            DATA_DIR / "fama_french_factors.parquet",
        ],
        "file_dep": ["./src/settings.py", "./src/pull_fred.py"],
        "clean": [],
    }

    # CRSP DSI: daily S&P 500 index level + returns
    # pull_crsp_spindx_level.py is preferred over pull_crsp_spx.py because
    # it includes the spindx column needed for strip price calculation.
    yield {
        "name": "crsp_spx_index",
        "doc": "Pull daily S&P 500 index level and returns from CRSP (crsp.dsi)",
        "actions": [
            "python ./src/settings.py",
            "python ./src/pull_crsp_spindx_level.py",
        ],
        "targets": [DATA_DIR / "crsp_sp500_daily.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_crsp_spindx_level.py"],
        "clean": [],
    }

    # CRSP MCTI: monthly 2y and 10y Treasury bond returns
    yield {
        "name": "crsp_treasuries",
        "doc": "Pull 2y and 10y Treasury bond returns from CRSP (crsp.mcti)",
        "actions": [
            "python ./src/settings.py",
            "python ./src/pull_crsp_treasuries.py",
        ],
        "targets": [DATA_DIR / "crsp_treasury_returns.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_crsp_treasuries.py"],
        "clean": [],
    }

    # CRSP MSI: monthly value-weighted market return
    yield {
        "name": "crsp_market_return",
        "doc": "Pull monthly CRSP value-weighted market return (crsp.msi)",
        "actions": [
            "python ./src/settings.py",
            "python ./src/pull_market_return.py",
        ],
        "targets": [DATA_DIR / "crsp_market_return.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_market_return.py"],
        "clean": [],
    }

    # OptionMetrics: SPX options (raw + month-end) + zero coupon curve
    # Note: pull_optionmetrics.py duplicates this task and is not used here.
    yield {
        "name": "optionmetrics_spx",
        "doc": "Pull SPX options and zero coupon curve from OptionMetrics",
        "actions": [
            "python ./src/settings.py",
            "python ./src/pull_spx_options_and_zero_coupon.py",
        ],
        "targets": [
            DATA_DIR / "optionmetrics_spx_raw.parquet",
            DATA_DIR / "optionmetrics_spx_monthly.parquet",
            DATA_DIR / "optionmetrics_zero_curve.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pull_spx_options_and_zero_coupon.py",
        ],
        "clean": [],
    }

    # OptionMetrics SECPRD: SPX spot price
    yield {
        "name": "spx_spot",
        "doc": "Pull SPX spot price from OptionMetrics (optionm.secprd)",
        "actions": [
            "python ./src/settings.py",
            "python ./src/pull_spx_spot.py",
        ],
        "targets": [DATA_DIR / "spx_spot.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_spx_spot.py"],
        "clean": [],
    }


# ---------------------------------------------------------------------------
# task_clean_data: merge and clean raw pulls into analysis-ready files
# ---------------------------------------------------------------------------

def task_clean_data():
    """Clean and merge raw data into analysis-ready files"""
    return {
        "actions": [
            "python ./src/settings.py",
            "python ./src/clean_data.py",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/clean_data.py",
            DATA_DIR / "crsp_sp500_daily.parquet",
            DATA_DIR / "fred_treasury_rates.parquet",
            DATA_DIR / "fama_french_factors.parquet",
            DATA_DIR / "crsp_treasury_returns.parquet",
            DATA_DIR / "crsp_market_return.parquet",
            DATA_DIR / "optionmetrics_spx_monthly.parquet",
            DATA_DIR / "optionmetrics_zero_curve.parquet",
            DATA_DIR / "spx_spot.parquet",
        ],
        # Update these names if clean_data.py writes under different filenames
        "targets": [
            DATA_DIR / "clean_options.parquet",
            DATA_DIR / "clean_zero_curve.parquet",
            DATA_DIR / "clean_crsp_sp500_monthly.parquet",
            DATA_DIR / "fama_french_monthly.parquet",
        ],
        "task_dep": ["pull"],
        "clean": [],
    }


# ---------------------------------------------------------------------------
# task_calc_tbill: convert FRED annual yield → monthly T-bill return
# ---------------------------------------------------------------------------

def task_calc_tbill():
    """Calculate 1-month T-bill monthly return from FRED yield"""
    return {
        "actions": ["python ./src/calculate_1-month_T-bill_return.py"],
        "file_dep": [
            "./src/calculate_1-month_T-bill_return.py",
            DATA_DIR / "fred_treasury_rates.parquet",
        ],
        "targets": [DATA_DIR / "t_bill_1m_monthly_return.parquet"],
        "task_dep": ["pull:fred"],
        "clean": True,
    }


# ---------------------------------------------------------------------------
# task_calc_strip_simple: simple ATM put-call parity strip price
# ---------------------------------------------------------------------------

def task_calc_strip_simple():
    """Calculate simple 1-year dividend strip price (ATM put-call parity)"""
    return {
        "actions": ["python ./src/calculate_dividend_strip.py"],
        "file_dep": [
            "./src/calculate_dividend_strip.py",
            DATA_DIR / "optionmetrics_spx_monthly.parquet",
            DATA_DIR / "optionmetrics_zero_curve.parquet",
            DATA_DIR / "spx_spot.parquet",
        ],
        "targets": [DATA_DIR / "dividend_strip_1y.parquet"],
        "task_dep": ["pull:optionmetrics_spx", "pull:spx_spot"],
        "clean": True,
    }


# ---------------------------------------------------------------------------
# task_calc: main analysis pipeline
#   implied_rates → strip_prices → returns
# ---------------------------------------------------------------------------

def task_calc():
    """Run main analysis pipeline"""

    # Step 1: Option-implied interest rates
    # Inputs:  _data/clean_options.parquet
    #          _data/clean_zero_curve.parquet
    # Outputs: _data/calc/implied_rates.parquet
    #          _data/calc/implied_rates_1y.parquet
    #          _data/calc/zero_curve_1y.parquet
    yield {
        "name": "implied_rates",
        "doc": "Calculate option-implied rates (outer product 1996-2003, regression 2004+)",
        "actions": ["python ./src/calc_implied_rates.py"],
        "file_dep": [
            "./src/calc_implied_rates.py",
            DATA_DIR / "clean_options.parquet",
            DATA_DIR / "clean_zero_curve.parquet",
        ],
        "targets": [
            CALC_DIR / "implied_rates.parquet",
            CALC_DIR / "implied_rates_1y.parquet",
            CALC_DIR / "zero_curve_1y.parquet",
        ],
        "task_dep": ["clean_data"],
        "clean": True,
    }

    # Step 2: Dividend strip prices
    # Inputs:  _data/clean_options.parquet
    # Outputs: _data/calc/strip_prices.parquet    (one target per month)
    #          _data/calc/all_strip_prices.parquet (all date x exdate combos)
    yield {
        "name": "strip_prices",
        "doc": "Calculate dividend strip prices from SPX options",
        "actions": ["python ./src/calc_strip_prices.py"],
        "file_dep": [
            "./src/calc_strip_prices.py",
            DATA_DIR / "clean_options.parquet",
        ],
        "targets": [
            CALC_DIR / "strip_prices.parquet",
            CALC_DIR / "all_strip_prices.parquet",
        ],
        "task_dep": ["clean_data"],
        "clean": True,
    }

    # Step 3: Monthly strip and market returns
    # Inputs:  _data/calc/strip_prices.parquet
    #          _data/calc/all_strip_prices.parquet
    #          _data/clean_crsp_sp500_monthly.parquet
    #          _data/fama_french_monthly.parquet
    #          _data/crsp_treasury_returns.parquet
    # Output:  _data/calc/monthly_returns.parquet
    yield {
        "name": "returns",
        "doc": "Calculate monthly strip and market returns (raw + excess)",
        "actions": ["python ./src/calc_returns.py"],
        "file_dep": [
            "./src/calc_returns.py",
            CALC_DIR / "strip_prices.parquet",
            CALC_DIR / "all_strip_prices.parquet",
            DATA_DIR / "clean_crsp_sp500_monthly.parquet",
            DATA_DIR / "fama_french_monthly.parquet",
            DATA_DIR / "crsp_treasury_returns.parquet",
        ],
        "targets": [CALC_DIR / "monthly_returns.parquet"],
        "task_dep": ["calc:strip_prices", "calc:implied_rates"],
        "clean": True,
    }


# ---------------------------------------------------------------------------
# task_figures: generate all figures
# Savefig pattern: output/figure{N}/figure{N}.png
# ---------------------------------------------------------------------------

def task_figures():
    """Generate figures"""

    # fmt: off
    figure_scripts = {
        "figure1": {
            "scripts": ["./src/plot_figure1.py"],
            "outputs": [Path("output/figure1/figure1.png")],
        },
        "figure1_extension": {
            "scripts": ["./src/plot_figure1_extension.py"],
            "outputs": [Path("output/figure1/figure1_extension.png")],
        },
        "figure1_summary_stats": {
            "scripts": ["./src/plot_figure1_summary_stats.py"],
            "outputs": [Path("output/figure1/figure1_summary_stats.png")],
        },
        "figure2": {
            "scripts": ["./src/figure2.py"],
            "outputs": [Path("output/figure2/figure2.png")],
        },
        "figure2_extended": {
            "scripts": ["./src/figure2_extended.py"],
            "outputs": [Path("output/figure2/figure2_extended.png")],
        },
        "figure2_extended_winsorized": {
            "scripts": ["./src/figure2_extended_winsorized.py"],
            "outputs": [Path("output/figure2/figure2_extended_winsorized.png")],
        },
        "figure3": {
            "scripts": ["./src/figure3.py"],
            "outputs": [Path("output/figure3/figure3.png")],
        },
        "figure3_extended": {
            "scripts": ["./src/figure3_extended.py"],
            "outputs": [Path("output/figure3/figure3_extended.png")],
        },
        "graph2": {
            "scripts": ["./src/graph2.py"],
            "outputs": [Path("output/graph2/graph2.png")],
        },
        "graph3": {
            "scripts": ["./src/graph3.py"],
            "outputs": [Path("output/graph3/graph3.png")],
        },
    }
    # fmt: on

    for name, info in figure_scripts.items():
        yield {
            "name": name,
            "doc": f"Generate {name}",
            "actions": [f"python {s}" for s in info["scripts"]],
            "file_dep": info["scripts"],
            "targets": info["outputs"],
            "task_dep": ["calc"],
            "clean": True,
        }


# ---------------------------------------------------------------------------
# task_tables: generate all tables
# ---------------------------------------------------------------------------

def task_tables():
    """Generate tables"""

    # fmt: off
    table_scripts = {
        "table1": {
            "scripts": ["./src/table1.py"],
            "outputs": [OUTPUT_DIR / "table1.tex"],
        },
        "table1_extended": {
            "scripts": ["./src/table1_extended.py"],
            "outputs": [OUTPUT_DIR / "table1_extended.tex"],
        },
        "make_table1": {
            "scripts": ["./src/make_table1.py"],
            "outputs": [OUTPUT_DIR / "make_table1.tex"],
        },
        "latex_tables": {
            "scripts": ["./src/generate_latex_tables.py"],
            "outputs": [OUTPUT_DIR / "latex_tables.tex"],
        },
    }
    # fmt: on

    for name, info in table_scripts.items():
        yield {
            "name": name,
            "doc": f"Generate {name}",
            "actions": [f"python {s}" for s in info["scripts"]],
            "file_dep": info["scripts"],
            "targets": info["outputs"],
            "task_dep": ["calc"],
            "clean": True,
        }
