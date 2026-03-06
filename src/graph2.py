# make_figure2.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# =========================
# Paths
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "_data"

MARKET_FILE = DATA_DIR / "crsp_market_return.parquet"
STRIP_FILE = DATA_DIR / "dividend_strip_1y.parquet"
RF_FILE = DATA_DIR / "t_bill_1m_monthly_return.parquet"
TREASURY_FILE = DATA_DIR / "crsp_treasury_returns.parquet"

FIGURE_FILE = DATA_DIR / "figure2.png"

START_DATE = pd.Timestamp("1996-01-01")
END_DATE = pd.Timestamp("2022-12-31")

# =========================
# Load data
# =========================
print("Loading data...")

market = pd.read_parquet(MARKET_FILE)
strip = pd.read_parquet(STRIP_FILE)
rf = pd.read_parquet(RF_FILE)
treasury = pd.read_parquet(TREASURY_FILE)

# Restrict to sample period
market = market[(market.date >= START_DATE) & (market.date <= END_DATE)]
strip = strip[(strip.date >= START_DATE) & (strip.date <= END_DATE)]
rf = rf[(rf.date >= START_DATE) & (rf.date <= END_DATE)]
treasury = treasury[(treasury.date >= START_DATE) & (treasury.date <= END_DATE)]

# =========================
# Merge data
# =========================
df = market[['date','market_ret']]
df = df.merge(strip[['date','strip_ret']], on='date')
df = df.merge(rf[['date','tbill_1m_ret']], on='date')
df = df.merge(treasury[['date','treasury_2y_ret','treasury_10y_ret']], on='date')

# drop first month (论文 log return 会少一个)
df = df.iloc[1:].reset_index(drop=True)

# =========================
# Excess returns
# =========================
df["market_minus_rf"] = df["market_ret"] - df["tbill_1m_ret"]
df["strip_minus_rf"] = df["strip_ret"] - df["tbill_1m_ret"]

df["market_minus_10y"] = df["market_ret"] - df["treasury_10y_ret"]
df["strip_minus_2y"] = df["strip_ret"] - df["treasury_2y_ret"]

# =========================
# Cumulative returns
# =========================
df["cum_market"] = np.exp(df["market_ret"].cumsum())
df["cum_strip"] = np.exp(df["strip_ret"].cumsum())

df["cum_market_rf"] = np.exp(df["market_minus_rf"].cumsum())
df["cum_strip_rf"] = np.exp(df["strip_minus_rf"].cumsum())

df["cum_market_treas"] = np.exp(df["market_minus_10y"].cumsum())
df["cum_strip_treas"] = np.exp(df["strip_minus_2y"].cumsum())

# =========================
# Plot Figure 2
# =========================
print("Plotting Figure 2...")

fig, axes = plt.subplots(3,1, figsize=(8,10))

# Panel A
axes[0].plot(df["date"], df["cum_strip"], label="Dividend strip")
axes[0].plot(df["date"], df["cum_market"], label="Market")
axes[0].set_title("A. Cumulative returns")
axes[0].set_ylabel("Investment in $")
axes[0].legend()

# Panel B
axes[1].plot(df["date"], df["cum_strip_rf"], label="Dividend strip - rf")
axes[1].plot(df["date"], df["cum_market_rf"], label="Market - rf")
axes[1].set_title("B. Cumulative returns in excess of risk-free rate")
axes[1].set_ylabel("Investment in $")
axes[1].legend()

# Panel C
axes[2].plot(df["date"], df["cum_strip_treas"], label="Dividend strip - Treasury 2y")
axes[2].plot(df["date"], df["cum_market_treas"], label="Market - Treasury 10y")
axes[2].set_title("C. Cumulative returns in excess of Treasury returns")
axes[2].set_ylabel("Investment in $")
axes[2].legend()

plt.tight_layout()

# Save figure
plt.savefig(FIGURE_FILE, dpi=300)

print(f"Figure saved to {FIGURE_FILE}")

plt.show()
