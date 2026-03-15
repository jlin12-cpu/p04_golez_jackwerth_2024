import pandas as pd
import pytest

import pull_crsp_spindx_level as pull_crsp_spindx_level
import pull_crsp_treasuries as pull_crsp_treasuries
import pull_fred as pull_fred
import pull_spx_options_and_zero_coupon as pull_spx_options


class MockWRDSConnection:
    def __init__(self, df):
        self.df = df

    def raw_sql(self, query, date_cols=None):
        df = self.df.copy()
        if date_cols:
            for col in date_cols:
                df[col] = pd.to_datetime(df[col])
        return df

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def test_pull_spx_daily_returns_expected_columns(monkeypatch):
    mock_df = pd.DataFrame({
        "date": ["2020-01-02", "2020-01-03"],
        "spindx": [3257.85, 3234.85],
        "sprtrn": [0.0084, -0.0071],
        "vwretd": [0.0079, -0.0068],
        "vwretx": [0.0075, -0.0070],
    })

    monkeypatch.setattr(
        pull_crsp_spindx_level.wrds,
        "Connection",
        lambda wrds_username=None: MockWRDSConnection(mock_df),
    )

    df = pull_crsp_spindx_level.pull_spx_daily()

    expected_cols = ["date", "spindx", "sprtrn", "vwretd", "vwretx"]
    assert list(df.columns) == expected_cols
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert len(df) == 2


def test_pull_treasury_returns_expected_columns(monkeypatch):
    mock_df = pd.DataFrame({
        "date": ["2020-01-31", "2020-02-29"],
        "treasury_2y_ret": [0.001, 0.002],
        "treasury_10y_ret": [0.003, -0.001],
    })

    monkeypatch.setattr(
        pull_crsp_treasuries.wrds,
        "Connection",
        lambda wrds_username=None: MockWRDSConnection(mock_df),
    )

    df = pull_crsp_treasuries.pull_treasury_returns()

    expected_cols = ["date", "treasury_2y_ret", "treasury_10y_ret"]
    assert list(df.columns) == expected_cols
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert len(df) == 2


def test_pull_fred_rates_renames_and_scales_columns(monkeypatch):
    mock_raw = pd.DataFrame(
        {
            "DGS1": [5.0, 5.1],
            "DGS2": [4.8, 4.9],
            "DGS10": [4.2, 4.3],
        },
        index=pd.to_datetime(["2020-01-02", "2020-01-03"]),
    )
    mock_raw.index.name = "DATE"

    def mock_datareader(series_codes, source, start, end):
        assert source == "fred"
        return mock_raw

    monkeypatch.setattr(pull_fred.web, "DataReader", mock_datareader)

    df = pull_fred.pull_fred_rates()

    expected_cols = ["date", "treasury_1y", "treasury_2y", "treasury_10y"]
    assert list(df.columns) == expected_cols
    assert pd.api.types.is_datetime64_any_dtype(df["date"])

    assert df.loc[0, "treasury_1y"] == pytest.approx(0.05)
    assert df.loc[0, "treasury_2y"] == pytest.approx(0.048)
    assert df.loc[0, "treasury_10y"] == pytest.approx(0.042)


def test_pull_fama_french_full_renames_and_scales_columns(monkeypatch):
    mock_ff_daily = pd.DataFrame(
        {
            "Mkt-RF": [1.2, -0.5],
            "SMB": [0.3, -0.1],
            "HML": [0.4, 0.2],
            "RF": [0.02, 0.02],
        },
        index=pd.to_datetime(["2020-01-02", "2020-01-03"]),
    )
    mock_ff_daily.index.name = "Date"

    def mock_datareader(name, source, start, end):
        assert name == "F-F_Research_Data_Factors_daily"
        assert source == "famafrench"
        return {0: mock_ff_daily}

    monkeypatch.setattr(pull_fred.web, "DataReader", mock_datareader)

    df = pull_fred.pull_fama_french_full()

    expected_cols = ["date", "mkt_rf", "smb", "hml", "rf_1m"]
    assert list(df.columns) == expected_cols
    assert pd.api.types.is_datetime64_any_dtype(df["date"])

    assert df.loc[0, "mkt_rf"] == pytest.approx(0.012)
    assert df.loc[0, "smb"] == pytest.approx(0.003)
    assert df.loc[0, "hml"] == pytest.approx(0.004)
    assert df.loc[0, "rf_1m"] == pytest.approx(0.0002)


def test_pull_zero_coupon_expected_columns(monkeypatch, tmp_path):
    mock_df = pd.DataFrame({
        "date": ["2020-01-02", "2020-01-02"],
        "days": [30, 365],
        "rate": [0.05, 0.055],
    })

    monkeypatch.setattr(
        pull_spx_options.wrds,
        "Connection",
        lambda wrds_username=None: MockWRDSConnection(mock_df),
    )
    monkeypatch.setattr(pull_spx_options, "DATA_DIR", tmp_path)

    df = pull_spx_options.pull_zero_coupon()

    expected_cols = ["date", "days", "rate"]
    assert list(df.columns) == expected_cols
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert len(df) == 2

    out_file = tmp_path / "optionmetrics_zero_curve.parquet"
    assert out_file.exists()


def test_filter_month_end_options_keeps_last_trading_day(monkeypatch, tmp_path):
    monkeypatch.setattr(
        pull_spx_options,
        "OPTION_SPX_MONTHLY_FILE",
        tmp_path / "optionmetrics_spx_monthly.parquet",
    )

    df = pd.DataFrame({
        "date": pd.to_datetime([
            "2020-01-30", "2020-01-31",
            "2020-02-27", "2020-02-28", "2020-02-28",
        ]),
        "exdate": pd.to_datetime([
            "2020-05-01", "2020-05-01",
            "2020-06-01", "2020-06-01", "2020-06-15",
        ]),
        "cp_flag": ["C", "P", "C", "P", "C"],
        "strike": [3000, 3000, 3100, 3100, 3200],
        "best_bid": [10, 11, 12, 13, 14],
        "best_offer": [11, 12, 13, 14, 15],
        "mid_price": [10.5, 11.5, 12.5, 13.5, 14.5],
    })

    result = pull_spx_options.filter_month_end_options(df)

    expected_dates = pd.to_datetime(["2020-01-31", "2020-02-28"])
    assert set(result["date"].unique()) == set(expected_dates)

    jan_rows = result[result["date"] == pd.Timestamp("2020-01-31")]
    feb_rows = result[result["date"] == pd.Timestamp("2020-02-28")]

    assert len(jan_rows) == 1
    assert len(feb_rows) == 2

    assert (tmp_path / "optionmetrics_spx_monthly.parquet").exists()