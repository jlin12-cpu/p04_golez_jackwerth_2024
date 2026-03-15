"""
Unit tests for clean_data.py.

These tests verify that the clean-data layer produces tidy and correctly
transformed intermediate datasets from raw pulled inputs.

Test coverage includes:
1. Zero-curve cleaning: rates converted to decimals and tau created correctly.
2. Treasury-rate cleaning: APR-style rates converted to continuously
   compounded form.
3. Options cleaning: SPX index levels merged correctly and filtering rules
   enforced.
4. Monthly CRSP S&P 500 cleaning: monthly fields created with expected columns
   and month-end dates.
"""

import numpy as np
import pandas as pd
import pytest

import clean_data


def test_clean_zero_curve_creates_tau_and_decimal_rates():
    """
    clean_zero_curve should:
    - keep the expected columns,
    - convert rate from percent to decimal,
    - create tau = days / 365.
    """
    df_raw = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-02", "2020-01-02"]),
        "days": [30, 365],
        "rate": [5.0, 4.0],
    })

    df_clean = clean_data.clean_zero_curve(df_raw)

    expected_cols = ["date", "days", "rate", "tau"]
    assert list(df_clean.columns) == expected_cols

    assert df_clean.loc[0, "rate"] == pytest.approx(0.05)
    assert df_clean.loc[1, "rate"] == pytest.approx(0.04)
    assert df_clean.loc[0, "tau"] == pytest.approx(30 / 365)
    assert df_clean.loc[1, "tau"] == pytest.approx(1.0)


def test_clean_rates_converts_to_continuous_compounding():
    """
    clean_rates should convert Treasury rates from simple annualized form
    to continuously compounded form using log(1 + r).
    """
    df_raw = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-02"]),
        "rf_1m": [0.05],
        "treasury_1y": [0.06],
        "treasury_2y": [0.04],
        "treasury_10y": [0.03],
    })

    df_clean = clean_data.clean_rates(df_raw)

    # Depending on implementation, the cleaned function may overwrite
    # the original columns or create new ones. Handle both possibilities.
    treasury_1y_val = (
        df_clean.loc[0, "treasury_1y_cc"]
        if "treasury_1y_cc" in df_clean.columns
        else df_clean.loc[0, "treasury_1y"]
    )
    treasury_2y_val = (
        df_clean.loc[0, "treasury_2y_cc"]
        if "treasury_2y_cc" in df_clean.columns
        else df_clean.loc[0, "treasury_2y"]
    )
    treasury_10y_val = (
        df_clean.loc[0, "treasury_10y_cc"]
        if "treasury_10y_cc" in df_clean.columns
        else df_clean.loc[0, "treasury_10y"]
    )

    assert treasury_1y_val == pytest.approx(np.log(1.06), rel=1e-6)
    assert treasury_2y_val == pytest.approx(np.log(1.04), rel=1e-6)
    assert treasury_10y_val == pytest.approx(np.log(1.03), rel=1e-6)


def test_clean_options_merges_spindx_and_filters_rows():
    """
    clean_options should:
    - merge SPX index levels,
    - keep only options with mid_price >= 3,
    - keep only options with days_to_maturity >= 90,
    - keep only moneyness in [0.5, 1.5].
    """
    df_options = pd.DataFrame({
        "date": pd.to_datetime([
            "2020-01-31", "2020-01-31", "2020-01-31", "2020-01-31"
        ]),
        "exdate": pd.to_datetime([
            "2020-06-19", "2020-06-19", "2020-06-19", "2020-03-20"
        ]),
        "cp_flag": ["C", "P", "C", "P"],
        "strike": [3200, 3200, 7000, 3200],
        "best_bid": [10, 10, 10, 1],
        "best_offer": [12, 12, 12, 2],
        "mid_price": [11, 11, 11, 1.5],
        "days_to_maturity": [140, 140, 140, 49],
    })

    df_spx = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-31"]),
        "spindx": [3300],
    })

    df_clean = clean_data.clean_options(df_options, df_spx)

    assert "spindx" in df_clean.columns
    assert (df_clean["mid_price"] >= 3).all()
    assert (df_clean["days_to_maturity"] >= 90).all()

    moneyness = df_clean["strike"] / df_clean["spindx"]
    assert (moneyness >= 0.5).all()
    assert (moneyness <= 1.5).all()

    # Only the two strike=3200, dtm=140, mid_price=11 rows should remain
    assert len(df_clean) == 2


def test_clean_crsp_sp500_monthly_builds_monthly_fields():
    """
    clean_crsp_sp500_monthly should:
    - aggregate daily data to month-end,
    - keep expected monthly columns,
    - output one row per month.
    """
    df_daily = pd.DataFrame({
        "date": pd.to_datetime([
            "2020-01-30", "2020-01-31",
            "2020-02-27", "2020-02-28"
        ]),
        "spindx": [3270, 3280, 3290, 3300],
        "vwretd": [0.0010, 0.0020, 0.0030, 0.0040],
        "vwretx": [0.0008, 0.0015, 0.0025, 0.0030],
        "sprtrn": [0.0012, 0.0022, 0.0032, 0.0042],
    })

    df_monthly = clean_data.clean_spx_monthly(df_daily)

    assert "date" in df_monthly.columns
    assert "spindx" in df_monthly.columns
    assert "mkt_ret" in df_monthly.columns
    assert "div_monthly" in df_monthly.columns

    assert len(df_monthly) == 1
    assert set(df_monthly["date"]) == set(pd.to_datetime(["2020-02-29"]))