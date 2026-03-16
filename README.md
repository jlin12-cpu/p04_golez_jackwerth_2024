# Holding Period Effects in Dividend Strip Returns

## About this project

This repository contains our final project for **FINM 32900** at the University of Chicago.

We replicate key empirical results from:

> Golez, Benjamin, and Jens Jackwerth.  
> *Holding Period Effects in Dividend Strip Returns*.  
> *Review of Financial Studies* 37, no. 10 (2024): 3188–3215.

Our assigned project is **P04. Holding Period Effects in Dividend Strip Returns**.

Using an end-to-end reproducible analytical pipeline, we replicate and extend the following objects from the paper:

- **Figure 1**: 12-month interest rates
- **Figure 2**: cumulative returns
- **Figure 3**: annualized volatility across holding periods
- **Table 1**: monthly return summary statistics

We also extend these results through the most recent available sample in our pipeline (through **2024**).

This project is formatted using the **cookiecutter chartbook template** and automated using **PyDoit**.

---

## Team responsibilities

Both group members contributed to understanding the paper, designing the pipeline, validating results, and preparing the final report. Raw data pulls were a shared responsibility. Primary responsibilities were divided as follows.

### Jie Lin
- Collaborated on raw data pulls (CRSP, FRED, OptionMetrics)
- Built and tested major parts of the automated data and calculation pipeline
- Worked on strip return construction and diagnostics
- Implemented Figure 2 replication and extensions
- Implemented Figure 3 replication and extensions
- Collaborated on Table 1 replication and extensions
- Integrated figures and tables into the LaTeX report
- Helped test end-to-end reproducibility with PyDoit

### Zimeng Yi
- Collaborated on raw data pulls (CRSP, FRED, OptionMetrics)
- Worked on Figure 1 replication and extensions
- Collaborated on Table 1 replication and extensions
- Developed the data summary analysis 
- Developed the analysis tour notebook 
- Helped validate intermediate outputs and summary statistics
- Contributed to the report writeup and empirical interpretation
- Assisted with GitHub workflow, project organization, and debugging

---

## Replication goals

The paper studies dividend strip prices inferred from S&P 500 index options and emphasizes the importance of using **option-implied interest rates** rather than exogenous zero-curve rates. Our project reproduces the paper's main empirical objects and then re-runs the same calculations on an updated sample.

Our replication pipeline performs the following steps:

1. Pull raw data from WRDS, FRED, and the Fama-French Data Library
2. Clean the raw data into tidy intermediate datasets
3. Estimate option-implied interest rates
4. Construct dividend strip prices using put-call parity
5. Build monthly strip and market return panels
6. Generate final figures, tables, and the LaTeX report

---

## Quick start

### 1. Install LaTeX

You must have **TeX Live** (or another LaTeX distribution) installed and available in your path.

Installers:
- [Windows](https://tug.org/texlive/windows.html#install)
- [Mac](https://tug.org/mactex/mactex-download.html)

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate      # On Windows: .venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create a `.env` file

Copy `.env.example` into a new file called `.env` in the project root:

```bash
cp .env.example .env
```

Then edit `.env` with your own settings. A typical file looks like:

```
WRDS_USERNAME=your_wrds_username
START_DATE=1996-01-01
END_DATE=2025-12-31
```

### 5. Run the full pipeline

```bash
doit all
```

This executes the project end-to-end: raw data pulls → cleaning → intermediate calculations → figure and table generation → report compilation → chartbook site build.

### 6. List available tasks

```bash
doit list
```

---

## Main PyDoit tasks

| Task | Description |
|------|-------------|
| `pull_crsp_spindx` | Pull daily S&P 500 index data from CRSP |
| `pull_crsp_treasuries` | Pull monthly Treasury returns from CRSP |
| `pull_fred` | Pull Treasury rates and Fama-French factors |
| `pull_optionmetrics` | Pull SPX options and zero curve from OptionMetrics |
| `clean_data` | Clean and standardize all raw data |
| `calc_implied_rates` | Estimate option-implied interest rates |
| `calc_strip_prices` | Construct dividend strip prices |
| `calc_returns` | Build monthly return panel |
| `plot_figure1` | Replicate Figure 1 |
| `plot_figure1_extension` | Extend Figure 1 through 2024 |
| `figure2` | Replicate Figure 2 |
| `figure2_extended` | Extend Figure 2 through 2024 |
| `figure2_extended_winsorized` | Robustness version of extended Figure 2 |
| `figure3` | Replicate Figure 3 |
| `figure3_extended` | Extend Figure 3 through 2024 |
| `table1` | Replicate Table 1 |
| `table1_extended` | Extend Table 1 through 2024 |
| `own_analysis` | Generate data summary figures and table for report |
| `run_tests` | Run all unit tests |
| `compile_report` | Compile LaTeX report to PDF |
| `generate_chartbook_html` | Generate self-contained HTML pages for chartbook |
| `build_chartbook_site` | Build chartbook website into docs/ |
| `all` | Run the full pipeline end-to-end |

---

## Data sources

| Source | Content |
|--------|---------|
| OptionMetrics via WRDS | European SPX options; zero-coupon curve |
| CRSP via WRDS | Daily S&P 500 index data; monthly Treasury bond returns |
| FRED | Treasury constant maturity rates |
| Kenneth French Data Library | Daily and monthly Fama-French factors; 1-month risk-free rate |

---

## Directory structure

```
src/                  Python scripts for pulling, cleaning, calculating, and plotting
_data/                Raw, cleaned, and intermediate parquet files (not tracked in Git)
_data/calc/           Derived datasets (implied rates, strip prices, monthly returns)
output/               Generated figures and tables
reports/              LaTeX report source files
docs/                 Chartbook website (auto-generated by doit build_chartbook_site)
assets/               Static files not generated automatically
```

### Why `_data/` is not tracked in Git

Most files in `_data/` are either automatically reproducible by rerunning the pipeline, or they come from licensed academic databases (WRDS) and cannot be redistributed.

---

## Naming conventions

| Prefix | Purpose |
|--------|---------|
| `pull_*.py` | Pull raw data from an external source |
| `clean_*.py` | Clean and standardize raw data |
| `calc_*.py` | Compute derived datasets |
| `figure*.py` / `table*.py` / `plot_*.py` | Generate final output figures and tables |

---

## Notebook tour

The project includes two notebooks:

- `src/analysis_tour.ipynb` — guided tour of the cleaned data and replication pipeline, walking through key intermediate outputs and methodology steps
- `src/own_analysis.ipynb` — data summary statistics and input data visualization; also generates `output/data_summary.tex`, `output/figure_A_input_series.png`, and `output/figure_B_options_coverage.png` used in the report's Data Summary section

The final official outputs are generated by the Python scripts and the PyDoit pipeline, not by manual notebook execution.

---

## Unit tests

This project uses unit tests to validate key intermediate logic and replicated values, including checks on Figure 1 summary statistics, Table 1 values, Figure 2 terminal values, and Figure 3 holding-period volatility values.

```bash
pytest                    # Run all tests
pytest --doctest-modules  # Include doctests
```

---

## Setting environment variables

If you want to export your `.env` variables into your shell session:

**Linux / Mac:**
```bash
set -a
source .env
set +a
```

**Windows (PowerShell):**
```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
    }
}
```

---

## Code formatting

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
ruff check . --fix
ruff format .
```

---

## Notes on data limitations

The original paper uses intraday CBOE data for part of the sample. Our replication uses WRDS-accessed OptionMetrics end-of-day data, which leads to some remaining quantitative differences, especially in strip return statistics:

- **Figure 1** is replicated closely
- **Market return results** are generally close to the paper
- **Strip return results** are directionally and qualitatively similar, but can differ more in magnitude
- **Post-2022 strip extensions** become less stable and should be interpreted cautiously

These issues are discussed in detail in the report.

---

## Repository hygiene

This repository does not contain:
- Copyrighted raw data
- API keys or private credentials
- Committed `.env` files

Use `.env.example` as the template for your local `.env` file.