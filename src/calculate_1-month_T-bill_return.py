# ============================================================
# Calculate 1-Month T-Bill Return (From FRED Yield)
# ============================================================

from pathlib import Path
import pandas as pd
import numpy as np

# ============================================================
# Paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "_data"

INPUT_FILE = DATA_DIR / "fred_treasury_rates.parquet"
OUTPUT_FILE = DATA_DIR / "t_bill_1m_monthly_return.parquet"

# ============================================================
# Load FRED Data
# ============================================================

def load_fred_data():

    print("Loading FRED treasury rates...")
    df = pd.read_parquet(INPUT_FILE)

    df["date"] = pd.to_datetime(df["date"])

    return df


# ============================================================
# Convert yield to monthly return
# ============================================================

def calculate_monthly_return(df):

    # Keep only needed columns
    df = df[["date", "rf_1m"]].copy()

    # Convert to month-end
    df["year_month"] = df["date"].dt.to_period("M")
    df = df.groupby("year_month").last().reset_index()

    df["date"] = df["year_month"].dt.to_timestamp("M")
    df.drop(columns=["year_month"], inplace=True)

    # rf_1m is annualized yield in decimal
    # Convert to monthly simple return
    df["tbill_1m_ret"] = df["rf_1m"] / 12

    df = df[["date", "tbill_1m_ret"]]

    return df


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    print("=" * 70)
    print("CALCULATING 1-MONTH T-BILL RETURN (FROM FRED)")
    print("=" * 70)

    df = load_fred_data()
    df_monthly = calculate_monthly_return(df)

    df_monthly.to_parquet(OUTPUT_FILE, index=False)

    print(f"Saved to: {OUTPUT_FILE}")
    print("Done!")
