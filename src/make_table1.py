# make_table1.py
import pandas as pd
import numpy as np
from pathlib import Path
import statsmodels.api as sm

# =========================
# Paths
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "_data"

MARKET_FILE = DATA_DIR / "crsp_market_return.parquet"
STRIP_FILE = DATA_DIR / "dividend_strip_1y.parquet"
RF_FILE = DATA_DIR / "t_bill_1m_monthly_return.parquet"
TREASURY_FILE = DATA_DIR / "crsp_treasury_returns.parquet"

START_DATE = pd.Timestamp("1996-01-01")
END_DATE = pd.Timestamp("2024-12-31")

# =========================
# Helper functions
# =========================
def log_return(price_series):
    return np.log(price_series).diff()

def annualize_return(r_monthly):
    return r_monthly.mean() * 12

def annualize_vol(r_monthly):
    return r_monthly.std() * np.sqrt(12)

def sharpe_ratio(r_monthly):
    return annualize_return(r_monthly) / annualize_vol(r_monthly)

def ar1_coef(r_monthly):
    r = r_monthly.dropna().values
    if len(r) < 2:
        return np.nan
    # OLS 回归 r_t ~ r_{t-1}，用 numpy 数组避免索引对齐问题
    model = sm.OLS(r[1:], sm.add_constant(r[:-1])).fit()
    return model.params[1]

# =========================
# Load and align data
# =========================
print("Loading data...")
market = pd.read_parquet(MARKET_FILE)
strip = pd.read_parquet(STRIP_FILE)
rf = pd.read_parquet(RF_FILE)
treasury = pd.read_parquet(TREASURY_FILE)

# Restrict to analysis period (论文: 1996-01 to 2022-12)
market = market[(market.date >= START_DATE) & (market.date <= END_DATE)].copy()
strip = strip[(strip.date >= START_DATE) & (strip.date <= END_DATE)].copy()
rf = rf[(rf.date >= START_DATE) & (rf.date <= END_DATE)].copy()
treasury = treasury[(treasury.date >= START_DATE) & (treasury.date <= END_DATE)].copy()

# Merge data on date
df = market[['date', 'market_ret']].copy()
df = df.merge(strip[['date', 'strip_ret']], on='date')
df = df.merge(rf[['date', 'tbill_1m_ret']], on='date')
df = df.merge(treasury[['date', 'treasury_2y_ret','treasury_10y_ret']], on='date')

# =========================
# Drop first row for log return calculation
# =========================
df = df.iloc[1:].reset_index(drop=True)

# =========================
# Compute excess returns
# =========================
df['market_minus_rf'] = df['market_ret'] - df['tbill_1m_ret']
df['strip_minus_rf'] = df['strip_ret'] - df['tbill_1m_ret']
df['market_minus_10y'] = df['market_ret'] - df['treasury_10y_ret']
df['strip_minus_2y'] = df['strip_ret'] - df['treasury_2y_ret']

# =========================
# Compute Table 1 stats
# =========================
columns = ['market_ret', 'strip_ret', 
           'market_minus_rf', 'strip_minus_rf',
           'market_minus_10y','strip_minus_2y']

results = []
for col in columns:
    series = df[col].dropna()
    mean = annualize_return(series) * 100  # percent
    vol = annualize_vol(series) * 100      # percent
    sharpe = sharpe_ratio(series)
    ar1 = ar1_coef(series)
    n_obs = len(series)
    results.append([mean, vol, sharpe, ar1, n_obs])

table1 = pd.DataFrame(results, 
                      index=['Market','Strip','Market-rf','Strip-rf','Market-10y','Strip-2y'],
                      columns=['Mean (%)','Std. dev (%)','Sharpe','AR(1)','N'])

# Round for display
table1 = table1.round(2)
print("\nTable 1 - Monthly Returns (Annualized):")
print(table1)

# =========================
# Save Table 1
# =========================
# TABLE1_FILE = DATA_DIR / "table1.csv"
# table1.to_csv(TABLE1_FILE)
# print(f"\nTable 1 saved to {TABLE1_FILE}")
