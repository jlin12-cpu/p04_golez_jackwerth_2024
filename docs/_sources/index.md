# Holding Period Effects in Dividend Strip Returns

Last updated: {sub-ref}`today` 


## Table of Contents

```{toctree}
:maxdepth: 1
:caption: Notebooks 📖

```



```{toctree}
:maxdepth: 1
:caption: Pipeline Charts 📈
charts.md
```

```{postlist}
:format: "{title}"
```


```{toctree}
:maxdepth: 1
:caption: Pipeline Dataframes 📊
dataframes/P04/all_strip_prices.md
dataframes/P04/clean_crsp_sp500_monthly.md
dataframes/P04/clean_options.md
dataframes/P04/clean_rates.md
dataframes/P04/clean_zero_curve.md
dataframes/P04/crsp_sp500_daily.md
dataframes/P04/crsp_treasury_returns.md
dataframes/P04/fama_french_factors.md
dataframes/P04/fama_french_monthly.md
dataframes/P04/fred_treasury_rates.md
dataframes/P04/implied_rates.md
dataframes/P04/implied_rates_1y.md
dataframes/P04/monthly_returns.md
dataframes/P04/optionmetrics_spx_monthly.md
dataframes/P04/optionmetrics_spx_raw.md
dataframes/P04/optionmetrics_zero_curve.md
dataframes/P04/strip_prices.md
dataframes/P04/zero_curve_1y.md
```


```{toctree}
:maxdepth: 1
:caption: Appendix 💡
myst_markdown_demos.md
apidocs/index
```


## Pipeline Specs
| Pipeline Name                   | Holding Period Effects in Dividend Strip Returns                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [P04](./index.md)              |
| Lead Pipeline Developer         | Jie Lin and Zimeng Yi             |
| Contributors                    | Jie Lin, Zimeng Yi           |
| Git Repo URL                    |                         |
| Pipeline Web Page               | <a href="file:///Users/jielin/Desktop/full_stack/p04_golez_jackwerth_2024/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-03-15 22:48:10           |
| OS Compatibility                |  |
| Linked Dataframes               |  [P04:crsp_sp500_daily](./dataframes/P04/crsp_sp500_daily.md)<br>  [P04:crsp_treasury_returns](./dataframes/P04/crsp_treasury_returns.md)<br>  [P04:fred_treasury_rates](./dataframes/P04/fred_treasury_rates.md)<br>  [P04:fama_french_factors](./dataframes/P04/fama_french_factors.md)<br>  [P04:fama_french_monthly](./dataframes/P04/fama_french_monthly.md)<br>  [P04:optionmetrics_spx_raw](./dataframes/P04/optionmetrics_spx_raw.md)<br>  [P04:optionmetrics_spx_monthly](./dataframes/P04/optionmetrics_spx_monthly.md)<br>  [P04:optionmetrics_zero_curve](./dataframes/P04/optionmetrics_zero_curve.md)<br>  [P04:clean_options](./dataframes/P04/clean_options.md)<br>  [P04:clean_zero_curve](./dataframes/P04/clean_zero_curve.md)<br>  [P04:clean_rates](./dataframes/P04/clean_rates.md)<br>  [P04:clean_crsp_sp500_monthly](./dataframes/P04/clean_crsp_sp500_monthly.md)<br>  [P04:implied_rates](./dataframes/P04/implied_rates.md)<br>  [P04:implied_rates_1y](./dataframes/P04/implied_rates_1y.md)<br>  [P04:zero_curve_1y](./dataframes/P04/zero_curve_1y.md)<br>  [P04:strip_prices](./dataframes/P04/strip_prices.md)<br>  [P04:all_strip_prices](./dataframes/P04/all_strip_prices.md)<br>  [P04:monthly_returns](./dataframes/P04/monthly_returns.md)<br>  |




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

Both group members contributed to understanding the paper, designing the pipeline, validating results, and preparing the final report. Primary responsibilities were divided as follows.

### Jie Lin
- Built and tested major parts of the automated data and calculation pipeline
- Worked on strip return construction and diagnostics
- Implemented Figure 2 replication and extensions
- Implemented Figure 3 replication and extensions
- Integrated figures and tables into the LaTeX report
- Helped test end-to-end reproducibility with PyDoit

### Zimeng Yi
- Worked on Figure 1 replication and extensions
- Worked on Table 1 replication and extensions
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

This executes the project end-to-end: raw data pulls → cleaning → intermediate calculations → figure and table generation.

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
| `clean` | Clean and standardize all raw data |
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

The project includes notebooks that provide a tour of the cleaned data and selected parts of the analysis logic. These notebooks help the reader understand the structure of the data, how the paper's methodology is implemented, and how key replication objects were validated before scripting.

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