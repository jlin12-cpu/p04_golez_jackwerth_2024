"""
Unit tests for Figure 3 replication (Golez & Jackwerth 2024).

Test structure
--------------
Layer 1 – Structure tests
    Verify output files exist, the series CSV has the expected shape and
    columns, holding periods are correct, and no missing values are present.

Layer 2 – Helper-function unit tests
    Verify calc_annualized_std on synthetic data with known analytical values.

Layer 3 – Replication accuracy tests
    Compare annualized standard deviations against paper-reported values.
    Market-side series use tight tolerances. Strip-side uses wider tolerances
    at short horizons (data-driven deviation documented in Table 1 / Figure 3
    note) and tight tolerances at long horizons where the two series converge.

    Key finding being tested: strip volatility drops sharply from ~32-40%
    at h=1 to ~12% at h=36, while market volatility stays broadly flat.
    This convergence is the central evidence for measurement error in
    strip prices estimated from options data (Section 2.2 of the paper).

Paper values (Figure 3, Golez & Jackwerth 2024, read from figure)
-----------------------------------------------------------------
            h=1    h=6    h=12   h=18   h=24   h=30   h=36
Strip-rf:  ~32%   ~18%   ~14%   ~13%   ~12%   ~12%   ~12%
Mkt-rf:    ~16%   ~16%   ~17%   ~18%   ~18%   ~18%   ~18%
Strip-2y:  ~32%   ~19%   ~15%   ~14%   ~13%   ~13%   ~13%
Mkt-10y:   ~19%   ~20%   ~20%   ~21%   ~21%   ~20%   ~20%

Our values (data-driven deviation on strip side at short horizons):
            h=1    h=6    h=12   h=18   h=24   h=30   h=36
Strip-rf:  40.5%  21.9%  16.9%  15.0%  14.2%  13.0%  12.7%
Mkt-rf:    16.3%  17.1%  17.7%  17.7%  17.5%  17.0%  16.5%
Strip-2y:  40.5%  22.3%  17.3%  15.6%  14.9%  13.8%  13.5%
Mkt-10y:   18.7%  20.4%  20.5%  20.5%  20.3%  19.3%  18.4%
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import numpy as np
import pandas as pd
import pytest

from figure3 import calc_annualized_std

# ============================================================
# Paths and constants
# ============================================================

OUTPUT_DIR  = Path("output/figure3")
SERIES_CSV  = OUTPUT_DIR / "figure3_series.csv"
FIGURE_PNG  = OUTPUT_DIR / "figure3.png"

HOLDING_PERIODS = [1, 6, 12, 18, 24, 30, 36]

# Paper values read from Figure 3 (approximate, used only for direction tests)
PAPER_STRIP_RF  = {1: 32, 6: 18, 12: 14, 18: 13, 24: 12, 30: 12, 36: 12}
PAPER_MKT_RF    = {1: 16, 6: 16, 12: 17, 18: 18, 24: 18, 30: 18, 36: 18}


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture(scope="module")
def df_series():
    """Load the saved Figure 3 standard-deviation series CSV."""
    return pd.read_csv(SERIES_CSV)


@pytest.fixture(scope="module")
def df_returns_paper():
    """Load monthly returns filtered to the paper sample (1996-2022)."""
    path = Path("_data/calc/monthly_returns.parquet")
    df = pd.read_parquet(path)
    mask = (df["date"] >= "1996-01-01") & (df["date"] <= "2022-12-31")
    df = df.loc[mask].dropna(
        subset=["strip_ret_rf", "mkt_ret_rf", "strip_ret_2y", "mkt_ret_10y"]
    ).reset_index(drop=True)
    return df


@pytest.fixture(scope="module")
def df_stds(df_returns_paper):
    """Recompute standard deviations from actual data."""
    from figure3 import calc_all_stds
    return calc_all_stds(df_returns_paper)


# ============================================================
# Layer 1: Structure tests
# ============================================================

class TestFigure3Structure:

    def test_output_files_exist(self):
        """Both output files (PNG and CSV) must exist."""
        assert SERIES_CSV.exists(), f"Missing: {SERIES_CSV}"
        assert FIGURE_PNG.exists(), f"Missing: {FIGURE_PNG}"

    def test_series_csv_row_count(self, df_series):
        """CSV must have exactly one row per holding period (7 rows)."""
        assert len(df_series) == 7, (
            f"Expected 7 rows (one per holding period), got {len(df_series)}"
        )

    def test_series_csv_columns(self, df_series):
        """CSV must contain the expected columns."""
        required = {
            "holding_period",
            "std_strip_rf", "std_mkt_rf",
            "std_strip_2y", "std_mkt_10y",
        }
        assert required.issubset(df_series.columns), (
            f"Missing columns: {required - set(df_series.columns)}"
        )

    def test_holding_periods_correct(self, df_series):
        """Holding periods in CSV must match [1, 6, 12, 18, 24, 30, 36]."""
        assert list(df_series["holding_period"]) == HOLDING_PERIODS, (
            f"Holding periods: {list(df_series['holding_period'])}"
        )

    def test_no_missing_values(self, df_series):
        """No missing values in the standard deviation columns."""
        cols = ["std_strip_rf", "std_mkt_rf", "std_strip_2y", "std_mkt_10y"]
        missing = df_series[cols].isna().sum().sum()
        assert missing == 0, f"Found {missing} missing values"

    def test_all_values_positive(self, df_series):
        """All annualized standard deviations must be positive."""
        cols = ["std_strip_rf", "std_mkt_rf", "std_strip_2y", "std_mkt_10y"]
        for col in cols:
            assert (df_series[col] > 0).all(), (
                f"Non-positive values found in '{col}'"
            )

    def test_all_values_in_plausible_range(self, df_series):
        """All annualized standard deviations must be in [5%, 60%]."""
        cols = ["std_strip_rf", "std_mkt_rf", "std_strip_2y", "std_mkt_10y"]
        for col in cols:
            assert df_series[col].between(5, 60).all(), (
                f"Values in '{col}' outside [5%, 60%]: {df_series[col].tolist()}"
            )


# ============================================================
# Layer 2: Helper-function unit tests
# ============================================================

class TestCalcAnnualizedStd:

    def test_zero_returns_give_zero_std(self):
        """
        A constant (zero) return series should produce zero standard deviation
        at every holding period.
        """
        s = pd.Series(np.zeros(100))
        result = calc_annualized_std(s, HOLDING_PERIODS)
        for h, val in zip(HOLDING_PERIODS, result):
            assert val == pytest.approx(0.0, abs=1e-10), (
                f"Expected 0.0 std for h={h} with zero returns, got {val}"
            )

    def test_known_iid_std(self):
        """
        For iid monthly log returns with std = sigma,
        annualized_std(h) = sigma * sqrt(12) regardless of h.
        With a large synthetic sample this should hold approximately.
        """
        rng = np.random.default_rng(42)
        sigma = 0.04  # 4% monthly std
        n = 50_000
        s = pd.Series(rng.normal(0.0, sigma, n))

        expected = sigma * np.sqrt(12) * 100  # annualized, in percent
        result = calc_annualized_std(s, HOLDING_PERIODS)

        for h, val in zip(HOLDING_PERIODS, result):
            assert abs(val - expected) < 1.0, (
                f"h={h}: expected ~{expected:.2f}%, got {val:.2f}% "
                f"(iid std should be horizon-invariant)"
            )

    def test_negatively_autocorrelated_std_decreases_with_h(self):
        """
        A negatively autocorrelated series (like strip returns under
        measurement error) should have annualized std that decreases
        as holding period h increases.
        """
        rng = np.random.default_rng(1)
        # MA(1) with theta = -0.5 → strong negative autocorrelation
        eps = rng.normal(0, 1, 5000)
        x = eps[1:] - 0.5 * eps[:-1]
        s = pd.Series(x)

        result = calc_annualized_std(s, HOLDING_PERIODS)

        for i in range(len(result) - 1):
            assert result[i] >= result[i + 1], (
                f"Std should decrease with h for negatively autocorrelated series: "
                f"h={HOLDING_PERIODS[i]} gives {result[i]:.4f}%, "
                f"h={HOLDING_PERIODS[i+1]} gives {result[i+1]:.4f}%"
            )

    def test_positively_autocorrelated_std_increases_with_h(self):
        """
        A positively autocorrelated series should have annualized std that
        increases as holding period h increases.
        """
        rng = np.random.default_rng(2)
        n = 5000
        x = [0.0]
        for _ in range(n - 1):
            x.append(0.8 * x[-1] + rng.normal(0, 0.1))
        s = pd.Series(x)

        result = calc_annualized_std(s, [1, 6, 12, 24, 36])
        assert result[0] < result[-1], (
            f"Std should increase with h for positively autocorrelated series: "
            f"h=1 gives {result[0]:.4f}%, h=36 gives {result[-1]:.4f}%"
        )

    def test_output_length_matches_holding_periods(self):
        """Output length must equal the number of requested holding periods."""
        s = pd.Series(np.random.normal(0, 0.04, 200))
        hp = [1, 3, 6, 12]
        result = calc_annualized_std(s, hp)
        assert len(result) == len(hp), (
            f"Expected {len(hp)} values, got {len(result)}"
        )

    def test_single_holding_period(self):
        """calc_annualized_std should work with a single holding period."""
        s = pd.Series(np.random.normal(0, 0.04, 100))
        result = calc_annualized_std(s, [12])
        assert len(result) == 1
        assert np.isfinite(result[0])
        assert result[0] > 0


# ============================================================
# Layer 3: Replication accuracy tests
# ============================================================

class TestFigure3ReplicationAccuracy:

    # ---------- Core finding: strip std drops sharply, market stays flat ----------

    def test_strip_rf_std_drops_from_h1_to_h36(self, df_stds):
        """
        Strip-rf annualized std must drop substantially from h=1 to h=36.
        Paper: ~32% → ~12% (drop of ~20pp).
        Ours:  ~40% → ~13% (drop of ~27pp).
        We require at least a 15pp drop as a conservative threshold.
        """
        std_h1  = df_stds.loc[df_stds["holding_period"] == 1,  "std_strip_rf"].iloc[0]
        std_h36 = df_stds.loc[df_stds["holding_period"] == 36, "std_strip_rf"].iloc[0]
        drop = std_h1 - std_h36
        assert drop >= 15.0, (
            f"Strip-rf std drop h=1→h=36 = {drop:.1f}pp, expected ≥ 15pp. "
            f"(h=1: {std_h1:.1f}%, h=36: {std_h36:.1f}%)"
        )

    def test_market_rf_std_stays_broadly_flat(self, df_stds):
        """
        Market-rf annualized std must stay within a narrow band across all h.
        Paper: roughly 15-18% across all holding periods.
        We require that max - min < 5pp.
        """
        mkt_stds = df_stds["std_mkt_rf"].values
        spread = mkt_stds.max() - mkt_stds.min()
        assert spread < 5.0, (
            f"Market-rf std range = {spread:.2f}pp across holding periods "
            f"(values: {[f'{v:.1f}' for v in mkt_stds]}). Expected < 5pp spread."
        )

    def test_strip_std_exceeds_market_std_at_h1(self, df_stds):
        """
        At h=1, strip-rf std must substantially exceed market-rf std.
        This is the measurement-error signature: paper ~32% vs ~16%.
        We require strip > market + 10pp at h=1.
        """
        strip_h1 = df_stds.loc[df_stds["holding_period"] == 1, "std_strip_rf"].iloc[0]
        mkt_h1   = df_stds.loc[df_stds["holding_period"] == 1, "std_mkt_rf"].iloc[0]
        assert strip_h1 > mkt_h1 + 10.0, (
            f"At h=1, strip-rf std ({strip_h1:.1f}%) should exceed "
            f"market-rf std ({mkt_h1:.1f}%) by at least 10pp"
        )

    def test_strip_std_converges_below_market_at_h36(self, df_stds):
        """
        By h=36, strip-rf std should fall below market-rf std.
        Paper: strip ~12% < market ~18%.
        This is the key result motivating longer holding periods.
        """
        strip_h36 = df_stds.loc[df_stds["holding_period"] == 36, "std_strip_rf"].iloc[0]
        mkt_h36   = df_stds.loc[df_stds["holding_period"] == 36, "std_mkt_rf"].iloc[0]
        assert strip_h36 < mkt_h36, (
            f"At h=36, strip-rf std ({strip_h36:.1f}%) should be below "
            f"market-rf std ({mkt_h36:.1f}%)"
        )

    def test_strip_std_monotonically_decreasing(self, df_stds):
        """
        Strip-rf annualized std should be monotonically decreasing
        across holding periods. This follows from negative autocorrelation
        induced by measurement error.
        """
        stds = df_stds["std_strip_rf"].values
        for i in range(len(stds) - 1):
            assert stds[i] >= stds[i + 1], (
                f"Strip-rf std not decreasing: "
                f"h={HOLDING_PERIODS[i]} gives {stds[i]:.2f}%, "
                f"h={HOLDING_PERIODS[i+1]} gives {stds[i+1]:.2f}%"
            )

    # ---------- Market-side: direct comparison with tight tolerance ----------

    def test_market_rf_std_h1_close_to_paper(self, df_stds):
        """Market-rf std at h=1 should be within 3pp of paper value (~16%)."""
        val   = df_stds.loc[df_stds["holding_period"] == 1, "std_mkt_rf"].iloc[0]
        paper = PAPER_MKT_RF[1]
        assert abs(val - paper) <= 3.0, (
            f"Market-rf std at h=1: ours={val:.1f}%, paper≈{paper}%, tol=±3pp"
        )

    def test_market_rf_std_h36_close_to_paper(self, df_stds):
        """Market-rf std at h=36 should be within 3pp of paper value (~18%)."""
        val   = df_stds.loc[df_stds["holding_period"] == 36, "std_mkt_rf"].iloc[0]
        paper = PAPER_MKT_RF[36]
        assert abs(val - paper) <= 3.0, (
            f"Market-rf std at h=36: ours={val:.1f}%, paper≈{paper}%, tol=±3pp"
        )

    # ---------- Strip-side: tight tolerance at h=36, wide at h=1 ----------

    def test_strip_rf_std_h1_in_plausible_range(self, df_stds):
        """
        Strip-rf std at h=1 should be in [25%, 55%].
        Paper: ~32%; ours: ~40.5% (data-driven deviation, same as Table 1 / Figure 2).
        """
        val = df_stds.loc[df_stds["holding_period"] == 1, "std_strip_rf"].iloc[0]
        assert 25.0 <= val <= 55.0, (
            f"Strip-rf std at h=1: {val:.1f}%, expected in [25%, 55%]"
        )

    def test_strip_rf_std_h36_close_to_paper(self, df_stds):
        """
        Strip-rf std at h=36 should be within 4pp of paper value (~12%).
        By h=36 the measurement error has dissipated, so convergence is expected.
        Paper: ~12%; ours: ~12.7%.
        """
        val   = df_stds.loc[df_stds["holding_period"] == 36, "std_strip_rf"].iloc[0]
        paper = PAPER_STRIP_RF[36]
        assert abs(val - paper) <= 4.0, (
            f"Strip-rf std at h=36: ours={val:.1f}%, paper≈{paper}%, tol=±4pp"
        )

    # ---------- Panel B (Treasury excess): same qualitative pattern ----------

    def test_strip_2y_std_drops_from_h1_to_h36(self, df_stds):
        """
        Strip-2y std must drop substantially from h=1 to h=36 (same as Panel A).
        """
        std_h1  = df_stds.loc[df_stds["holding_period"] == 1,  "std_strip_2y"].iloc[0]
        std_h36 = df_stds.loc[df_stds["holding_period"] == 36, "std_strip_2y"].iloc[0]
        assert std_h1 - std_h36 >= 15.0, (
            f"Strip-2y std drop h=1→h=36 = {std_h1 - std_h36:.1f}pp, expected ≥ 15pp"
        )

    def test_strip_2y_std_converges_below_market_10y_at_h36(self, df_stds):
        """
        By h=36, strip-2y std should fall below market-10y std.
        Paper: strip ~13% < market ~20%.
        """
        strip_h36 = df_stds.loc[df_stds["holding_period"] == 36, "std_strip_2y"].iloc[0]
        mkt_h36   = df_stds.loc[df_stds["holding_period"] == 36, "std_mkt_10y"].iloc[0]
        assert strip_h36 < mkt_h36, (
            f"At h=36, strip-2y std ({strip_h36:.1f}%) should be below "
            f"market-10y std ({mkt_h36:.1f}%)"
        )

    def test_mkt_10y_std_stable_across_horizons(self, df_stds):
        """
        Market-10y std should stay within a narrow band (< 5pp spread).
        """
        stds = df_stds["std_mkt_10y"].values
        spread = stds.max() - stds.min()
        assert spread < 5.0, (
            f"Market-10y std spread = {spread:.2f}pp, expected < 5pp"
        )

    # ---------- Internal consistency ----------

    def test_panel_a_and_b_strip_std_similar_at_h1(self, df_stds):
        """
        Strip-rf and strip-2y std at h=1 should be very close to each other
        (rf and 2y Treasury are both subtracted from the same strip return).
        """
        strip_rf = df_stds.loc[df_stds["holding_period"] == 1, "std_strip_rf"].iloc[0]
        strip_2y = df_stds.loc[df_stds["holding_period"] == 1, "std_strip_2y"].iloc[0]
        assert abs(strip_rf - strip_2y) < 2.0, (
            f"Strip-rf ({strip_rf:.2f}%) and Strip-2y ({strip_2y:.2f}%) "
            f"should be very close at h=1"
        )

    def test_csv_matches_recomputed_stds(self, df_series, df_stds):
        """
        The saved CSV must exactly match the recomputed standard deviations
        (up to floating-point precision).
        """
        cols = ["std_strip_rf", "std_mkt_rf", "std_strip_2y", "std_mkt_10y"]
        for col in cols:
            for h in HOLDING_PERIODS:
                csv_val = df_series.loc[
                    df_series["holding_period"] == h, col
                ].iloc[0]
                comp_val = df_stds.loc[
                    df_stds["holding_period"] == h, col
                ].iloc[0]
                assert abs(csv_val - comp_val) < 1e-6, (
                    f"CSV mismatch for '{col}' at h={h}: "
                    f"CSV={csv_val:.6f}%, recomputed={comp_val:.6f}%"
                )