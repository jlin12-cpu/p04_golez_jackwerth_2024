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

START_DATE = "1996-01-01"
END_DATE = "2022-12-31"

# =========================
# Load data
# =========================
print("Loading data...")

market = pd.read_parquet(MARKET_FILE)
strip = pd.read_parquet(STRIP_FILE)
rf = pd.read_parquet(RF_FILE)
treasury = pd.read_parquet(TREASURY_FILE)

market = market[(market.date >= START_DATE) & (market.date <= END_DATE)]
strip = strip[(strip.date >= START_DATE) & (strip.date <= END_DATE)]
rf = rf[(rf.date >= START_DATE) & (rf.date <= END_DATE)]
treasury = treasury[(treasury.date >= START_DATE) & (treasury.date <= END_DATE)]

df = market[['date','market_ret']]
df = df.merge(strip[['date','strip_ret']], on='date')
df = df.merge(rf[['date','tbill_1m_ret']], on='date')
df = df.merge(treasury[['date','treasury_2y_ret','treasury_10y_ret']], on='date')

# =========================
# Excess returns
# =========================
df['market_rf'] = df['market_ret'] - df['tbill_1m_ret']
df['strip_rf'] = df['strip_ret'] - df['tbill_1m_ret']

df['market_treas'] = df['market_ret'] - df['treasury_10y_ret']
df['strip_treas'] = df['strip_ret'] - df['treasury_2y_ret']

# =========================
# Holding periods
# =========================
holding_periods = [1,6,12,18,24,30,36]

def compute_vol(series, h):

    rolling = series.rolling(h).sum()
    vol = rolling.std() * np.sqrt(12/h)

    return vol

# store results
market_rf_vol = []
strip_rf_vol = []

market_treas_vol = []
strip_treas_vol = []

for h in holding_periods:

    market_rf_vol.append(compute_vol(df['market_rf'],h))
    strip_rf_vol.append(compute_vol(df['strip_rf'],h))

    market_treas_vol.append(compute_vol(df['market_treas'],h))
    strip_treas_vol.append(compute_vol(df['strip_treas'],h))

# convert to %
market_rf_vol = np.array(market_rf_vol)*100
strip_rf_vol = np.array(strip_rf_vol)*100
market_treas_vol = np.array(market_treas_vol)*100
strip_treas_vol = np.array(strip_treas_vol)*100

# =========================
# Plot
# =========================
fig, ax = plt.subplots(1,2, figsize=(12,5))

# Panel A
ax[0].plot(holding_periods, market_rf_vol, label="Market - rf")
ax[0].plot(holding_periods, strip_rf_vol, label="Dividend strip - rf")

ax[0].set_title("A Returns in excess of the risk-free rate")
ax[0].set_xlabel("Holding period in months")
ax[0].set_ylabel("Annualized volatility (%)")
ax[0].legend()

# Panel B
ax[1].plot(holding_periods, market_treas_vol, label="Market - Treasury 10y")
ax[1].plot(holding_periods, strip_treas_vol, label="Dividend strip - Treasury 2y")

ax[1].set_title("B Returns in excess of Treasury bond returns")
ax[1].set_xlabel("Holding period in months")
ax[1].set_ylabel("Annualized volatility (%)")
ax[1].legend()

plt.tight_layout()

FIG_FILE = DATA_DIR / "figure3.png"
plt.savefig(FIG_FILE, dpi=300)

print("Figure 3 saved:", FIG_FILE)
plt.show()
