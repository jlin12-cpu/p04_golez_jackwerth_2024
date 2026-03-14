"""
Unit tests for Figure 1 replication.

These tests verify:
1. The Figure 1 dataset has the expected structure and coverage.
2. The replicated summary statistics are reasonably close to the values
   reported in Golez & Jackwerth (2024).
3. The helper functions in calc_implied_rates.py behave as expected on
   simple synthetic examples.
4. The exported summary CSV is internally consistent with the plotted series.

Paper values:
- Average difference (implied - zero curve): 7 bp
- Full-sample relative difference: 2.82%
- First-half relative difference: 0.64%
- Second-half relative difference: 11.80%
"""
# conftest.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

import numpy as np
import pandas as pd
import pytest

from calc_implied_rates import calc_outer_product_rate, calc_regression_rate


# ============================================================
# Paths and sample dates
# ============================================================

CALC_DIR = Path("_data/calc")
OUTPUT_DIR = Path("output/figure1")

PAPER_START = pd.Timestamp("1996-01-01")
PAPER_END = pd.Timestamp("2022-12-31")
FIRST_HALF_END = pd.Timestamp("2009-12-31")
SECOND_HALF_START = pd.Timestamp("2010-01-01")


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def df_figure1():
    """
    Load the merged Figure 1 dataset used for plotting.
    """
    return pd.read_csv(OUTPUT_DIR / "figure1_series.csv", parse_dates=["date"])


@pytest.fixture
def df_summary():
    """
    Load the summary statistics exported by plot_figure1.py.
    """
    return pd.read_csv(OUTPUT_DIR / "figure1_summary.csv")


@pytest.fixture
def df_implied_1y():
    """
    Load the interpolated 1-year option-implied rate series.
    """
    return pd.read_parquet(CALC_DIR / "implied_rates_1y.parquet")


# ============================================================
# Tests: Data integrity
# ============================================================

def test_figure1_row_count(df_figure1):
    """
    Figure 1 should contain 324 monthly observations
    from Jan 1996 through Dec 2022.
    """
    assert len(df_figure1) == 324, f"Expected 324 rows, got {len(df_figure1)}"


def test_figure1_date_range(df_figure1):
    """
    Figure 1 should cover the paper sample only.
    """
    assert df_figure1["date"].min() >= PAPER_START, (
        f"Start date {df_figure1['date'].min()} is before {PAPER_START}"
    )
    assert df_figure1["date"].max() <= PAPER_END, (
        f"End date {df_figure1['date'].max()} is after {PAPER_END}"
    )


def test_figure1_dates_are_unique(df_figure1):
    """
    The final plotted Figure 1 dataset should have one row per month-end date.
    """
    assert df_figure1["date"].is_unique, "Duplicate dates found in Figure 1 series"


def test_no_missing_values(df_figure1):
    """
    Figure 1 dataset should not contain missing values
    in the plotted rate columns.
    """
    missing = df_figure1[["r_1y", "zero_1y", "treasury_1y"]].isna().sum().sum()
    assert missing == 0, "Missing values found in Figure 1 dataset"


def test_implied_rates_in_reasonable_range(df_implied_1y):
    """
    For the 1996-2022 replication sample, 1-year implied rates
    should stay within a plausible range.
    """
    assert df_implied_1y["r_1y"].min() >= 0.0, (
        f"Implied rate below 0%: {df_implied_1y['r_1y'].min():.4f}"
    )
    assert df_implied_1y["r_1y"].max() <= 0.15, (
        f"Implied rate above 15%: {df_implied_1y['r_1y'].max():.4f}"
    )


def test_zero_curve_in_reasonable_range(df_figure1):
    """
    For the 1996-2022 replication sample, the 1-year zero curve
    should remain within a plausible range.
    """
    assert df_figure1["zero_1y"].min() >= 0.0, (
        f"Zero curve below 0%: {df_figure1['zero_1y'].min():.4f}"
    )
    assert df_figure1["zero_1y"].max() <= 0.10, (
        f"Zero curve above 10%: {df_figure1['zero_1y'].max():.4f}"
    )


# ============================================================
# Tests: Summary file structure
# ============================================================

def test_summary_file_contains_expected_statistics(df_summary):
    """
    figure1_summary.csv should contain exactly the expected statistics.
    """
    expected_stats = {
        "avg_diff_bp",
        "rel_full_pct",
        "rel_first_half_pct",
        "rel_second_half_pct",
    }
    assert set(df_summary["statistic"]) == expected_stats, (
        f"Unexpected summary statistics found: {set(df_summary['statistic'])}"
    )


def test_summary_csv_matches_direct_calculation(df_figure1, df_summary):
    """
    The exported summary CSV should exactly match statistics
    computed directly from figure1_series.csv.
    """
    summary_map = dict(zip(df_summary["statistic"], df_summary["value"]))

    avg_diff_bp = (df_figure1["r_1y"] - df_figure1["zero_1y"]).mean() * 10000
    rel_full_pct = (
        (df_figure1["r_1y"] - df_figure1["zero_1y"]).mean()
        / df_figure1["zero_1y"].mean()
    ) * 100

    first_half = df_figure1[df_figure1["date"] <= FIRST_HALF_END]
    rel_first_half_pct = (
        (first_half["r_1y"] - first_half["zero_1y"]).mean()
        / first_half["zero_1y"].mean()
    ) * 100

    second_half = df_figure1[df_figure1["date"] >= SECOND_HALF_START]
    rel_second_half_pct = (
        (second_half["r_1y"] - second_half["zero_1y"]).mean()
        / second_half["zero_1y"].mean()
    ) * 100

    assert abs(summary_map["avg_diff_bp"] - avg_diff_bp) < 1e-10
    assert abs(summary_map["rel_full_pct"] - rel_full_pct) < 1e-10
    assert abs(summary_map["rel_first_half_pct"] - rel_first_half_pct) < 1e-10
    assert abs(summary_map["rel_second_half_pct"] - rel_second_half_pct) < 1e-10


# ============================================================
# Tests: Replication vs paper
# ============================================================

def test_avg_diff_bp_close_to_paper(df_figure1):
    """
    Average difference (implied - zero curve) should be within
    3 bp of the paper's reported 7 bp.
    """
    paper_value = 7.0
    tolerance = 3.0

    our_value = (df_figure1["r_1y"] - df_figure1["zero_1y"]).mean() * 10000

    assert abs(our_value - paper_value) <= tolerance, (
        f"avg_diff_bp = {our_value:.2f} bp, "
        f"paper = {paper_value:.2f} bp, "
        f"tolerance = ±{tolerance:.2f} bp"
    )


def test_rel_full_pct_close_to_paper(df_figure1):
    """
    Full-sample relative difference should be within 1 percentage point
    of the paper's reported 2.82%.
    """
    paper_value = 2.82
    tolerance = 1.0

    our_value = (
        (df_figure1["r_1y"] - df_figure1["zero_1y"]).mean()
        / df_figure1["zero_1y"].mean()
    ) * 100

    assert abs(our_value - paper_value) <= tolerance, (
        f"rel_full_pct = {our_value:.2f}%, "
        f"paper = {paper_value:.2f}%, "
        f"tolerance = ±{tolerance:.2f}%"
    )


def test_rel_first_half_pct_close_to_paper(df_figure1):
    """
    First-half relative difference should be within 1 percentage point
    of the paper's reported 0.64%.
    """
    paper_value = 0.64
    tolerance = 1.0

    first_half = df_figure1[df_figure1["date"] <= FIRST_HALF_END]
    our_value = (
        (first_half["r_1y"] - first_half["zero_1y"]).mean()
        / first_half["zero_1y"].mean()
    ) * 100

    assert abs(our_value - paper_value) <= tolerance, (
        f"rel_first_half_pct = {our_value:.2f}%, "
        f"paper = {paper_value:.2f}%, "
        f"tolerance = ±{tolerance:.2f}%"
    )


def test_rel_second_half_pct_close_to_paper(df_figure1):
    """
    Second-half relative difference should be within 2 percentage points
    of the paper's reported 11.80%.
    """
    paper_value = 11.80
    tolerance = 2.0

    second_half = df_figure1[df_figure1["date"] >= SECOND_HALF_START]
    our_value = (
        (second_half["r_1y"] - second_half["zero_1y"]).mean()
        / second_half["zero_1y"].mean()
    ) * 100

    assert abs(our_value - paper_value) <= tolerance, (
        f"rel_second_half_pct = {our_value:.2f}%, "
        f"paper = {paper_value:.2f}%, "
        f"tolerance = ±{tolerance:.2f}%"
    )


# ============================================================
# Tests: Methodology helper functions
# ============================================================

def test_outer_product_rate_positive():
    """
    calc_outer_product_rate should return a positive finite rate
    on a simple valid synthetic example.
    """
    pivot = pd.DataFrame(
        {
            "C": [20.0, 10.0],
            "P": [5.0, 13.0],
        },
        index=[100.0, 120.0],
    )
    pivot.index.name = "strike"

    tau = 1.0
    r = calc_outer_product_rate(pivot, tau)

    assert pd.notna(r), "Outer product rate returned NaN"
    assert np.isfinite(r), "Outer product rate is not finite"
    assert r > 0.0, f"Outer product rate should be positive, got {r:.6f}"


def test_regression_rate_recovers_known_rate():
    """
    calc_regression_rate should recover a known synthetic interest rate
    from a noiseless put-call parity example.
    """
    strikes = np.array([90.0, 95.0, 100.0, 105.0, 110.0])
    S = 100.0
    tau = 1.0
    r_true = 0.05

    beta_true = np.exp(-r_true * tau)
    p_div = 2.0

    # Build synthetic pivot so that:
    # S - C + P = p_div + beta_true * X
    call_prices = np.full_like(strikes, 10.0, dtype=float)
    put_prices = p_div + beta_true * strikes - S + call_prices

    pivot = pd.DataFrame(
        {"C": call_prices, "P": put_prices},
        index=strikes,
    )
    pivot.index.name = "strike"

    r_hat = calc_regression_rate(pivot, tau, S)

    assert pd.notna(r_hat), "Regression rate returned NaN"
    assert np.isfinite(r_hat), "Regression rate is not finite"
    assert abs(r_hat - r_true) < 0.01, (
        f"Recovered rate {r_hat:.4f} far from true rate {r_true:.4f}"
    )


# ============================================================
# Tests: Economic direction
# ============================================================

def test_implied_rate_positive_on_average(df_figure1):
    """
    The option-implied 1-year rate should be positive on average
    over the paper sample.
    """
    assert df_figure1["r_1y"].mean() > 0, "Average implied rate should be positive"


def test_implied_above_zero_curve_on_average(df_figure1):
    """
    Over the original sample (1996-2022), the implied rate should
    exceed the zero curve on average, consistent with the paper.
    """
    diff = (df_figure1["r_1y"] - df_figure1["zero_1y"]).mean()
    assert diff > 0, (
        f"Implied-minus-zero spread should be positive on average, "
        f"got {diff * 10000:.2f} bp"
    )