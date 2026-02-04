# pull_SPX_index_options.py
from pathlib import Path
import pandas as pd
import wrds
from decouple import config

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "_data"
DATA_DIR.mkdir(exist_ok=True)

WRDS_USERNAME = config("WRDS_USERNAME")
START_YEAR = int(config("START_YEAR", default=1996))
END_YEAR = int(config("END_YEAR", default=2024))

OUTPUT_FILE = DATA_DIR / "optionm_spx_options.parquet"

def connect_wrds():
    print("\nConnecting to WRDS...")
    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    print("Loading library list...")
    db.list_libraries()
    print("Done")
    return db

def pull_spx_options_by_year(db, year):
    table_name = f"opprcd{year}"

    # 获取表列名
    try:
        desc = db.describe_table(library="optionm", table=table_name)
        columns = desc["column_name"].tolist()
    except Exception as e:
        print(f"Skipped {year}: table does not exist ({e})")
        return pd.DataFrame()

    # 期望的列
    select_cols = ["date","exdate","cp_flag","strike_price","best_bid","best_offer","volume","open_interest","impl_volatility"]
    available_cols = [col for col in select_cols if col in columns]

    if not available_cols:
        print(f"Skipped {year}: none of the expected columns exist")
        return pd.DataFrame()

    # 确定筛选 SPX 的条件
    if "ticker" in columns:
        where_clause = "WHERE ticker='SPX'"
    elif "secid" in columns:
        # 从 optionmnames 找 SPX secid
        spx_secids = db.raw_sql("SELECT secid FROM optionm.optionmnames WHERE ticker='SPX'")["secid"].tolist()
        if spx_secids:
            ids_str = ",".join(str(sid) for sid in spx_secids)
            where_clause = f"WHERE secid IN ({ids_str})"
        else:
            where_clause = ""
    else:
        where_clause = ""  # 无法筛选，拉所有数据

    col_str = ", ".join(available_cols)
    query = f"SELECT {col_str} FROM optionm.{table_name} {where_clause}"

    try:
        df = db.raw_sql(query)
        if df.empty:
            print(f"Skipped {year}: no data")
            return pd.DataFrame()
        df["year"] = year
        print(f"Pulled {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        print(f"Skipped {year}: {e}")
        return pd.DataFrame()

def pull_spx_index_options(start_year=START_YEAR, end_year=END_YEAR):
    db = connect_wrds()
    all_dfs = []

    for year in range(start_year, end_year + 1):
        df_year = pull_spx_options_by_year(db, year)
        if not df_year.empty:
            all_dfs.append(df_year)

    if not all_dfs:
        raise RuntimeError("No SPX option data pulled.")

    full_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal rows pulled: {len(full_df)}")
    return full_df

if __name__ == "__main__":
    df = pull_spx_index_options()
    print("\nSaving data to:", OUTPUT_FILE)
    df.to_parquet(OUTPUT_FILE, index=False)
    print("Done.")
