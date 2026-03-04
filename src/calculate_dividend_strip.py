# ============================================================
# Calculate 1-Year Dividend Strip Price
# ============================================================

from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "_data"

OPTIONS_FILE = DATA_DIR / "optionmetrics_spx_monthly.parquet"
ZERO_FILE = DATA_DIR / "optionmetrics_zero_curve.parquet"
SPOT_FILE = DATA_DIR / "spx_spot.parquet"

OUTPUT_FILE = DATA_DIR / "dividend_strip_1y.parquet"

TARGET_DAYS = 365


# ============================================================
# Load Data
# ============================================================

def load_data():
    df_opt = pd.read_parquet(OPTIONS_FILE)
    df_zero = pd.read_parquet(ZERO_FILE)
    df_spot = pd.read_parquet(SPOT_FILE)

    df_opt["date"] = pd.to_datetime(df_opt["date"])
    df_zero["date"] = pd.to_datetime(df_zero["date"])
    df_spot["date"] = pd.to_datetime(df_spot["date"])

    return df_opt, df_zero, df_spot


# ============================================================
# Interpolate zero rate
# ============================================================

def get_zero_rate(df_zero, date, days):
    df_day = df_zero[df_zero["date"] == date]

    if df_day.empty:
        return np.nan

    return np.interp(
        days,
        df_day["days"],
        df_day["rate"]
    )


# ============================================================
# Calculate strip price
# ============================================================

def calculate_strip(df_opt, df_zero, df_spot):

    results = []

    for date in df_opt["date"].unique():

        df_day = df_opt[df_opt["date"] == date]

        # choose maturity closest to 365 days
        df_day["maturity_gap"] = abs(df_day["days_to_maturity"] - TARGET_DAYS)
        maturity = df_day.sort_values("maturity_gap")["days_to_maturity"].iloc[0]

        df_maturity = df_day[df_day["days_to_maturity"] == maturity]

        # merge calls and puts
        calls = df_maturity[df_maturity["cp_flag"] == "C"]
        puts = df_maturity[df_maturity["cp_flag"] == "P"]

        df_merge = pd.merge(
            calls,
            puts,
            on=["strike"],
            suffixes=("_C", "_P")
        )

        if df_merge.empty:
            continue

        # Get spot price
        S = df_spot[df_spot["date"] == date]["spx_price"].values
        if len(S) == 0:
            continue
        S = S[0]

        # Choose ATM strike
        df_merge["strike_gap"] = abs(df_merge["strike"] - S)
        row = df_merge.sort_values("strike_gap").iloc[0]

        C = row["mid_price_C"]
        P = row["mid_price_P"]
        K = row["strike"]

        r = get_zero_rate(df_zero, date, maturity)
        if np.isnan(r):
            continue

        T = maturity / 365

        PV_div = S - (C - P) - K * np.exp(-r * T)

        results.append([date, PV_div])

    df_strip = pd.DataFrame(results, columns=["date", "strip_price"])
    df_strip = df_strip.sort_values("date")

    return df_strip


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    print("Calculating dividend strip...")

    df_opt, df_zero, df_spot = load_data()

    df_strip = calculate_strip(df_opt, df_zero, df_spot)

    # Compute log return
    df_strip["strip_ret"] = np.log(
        df_strip["strip_price"] /
        df_strip["strip_price"].shift(1)
    )

    df_strip.to_parquet(OUTPUT_FILE, index=False)

    print(f"Saved dividend strip to {OUTPUT_FILE}")
    print("Done.")
