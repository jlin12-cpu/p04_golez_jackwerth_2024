def pull_spx_options_by_year(db, year):

    table = f"opprcd{year}"

    query = f"""
        SELECT
            o.date,
            o.exdate,
            o.cp_flag,
            o.strike_price/1000 AS strike,
            o.best_bid,
            o.best_offer,
            (o.best_bid + o.best_offer)/2 AS mid_price,
            o.volume,
            o.open_interest,
            s.close AS spx_close
        FROM optionm.{table} o

        JOIN optionm.secprd s
            ON o.date = s.date
            AND s.secid = 108105

        WHERE
            o.secid = 108105
            AND o.cp_flag IN ('C','P')
            AND o.best_bid > 0
            AND o.best_offer > 0
            AND (o.best_bid + o.best_offer)/2 >= 3
            AND ABS((o.strike_price/1000)/s.close) BETWEEN 0.5 AND 1.5
            AND (o.exdate - o.date) >= 90
    """

    df = db.raw_sql(query, date_cols=["date","exdate"])
    df["days_to_maturity"] = (df["exdate"] - df["date"]).dt.days

    return df

def pull_zero_curve(db):

    query = """
        SELECT *
        FROM optionm.zero_curve
        WHERE date BETWEEN '1996-01-01'
        AND '2022-12-31'
    """

    return db.raw_sql(query, date_cols=["date"])

def pull_spx_close(db):

    query = """
        SELECT date, close
        FROM optionm.secprd
        WHERE secid = 108105
        AND date BETWEEN '1996-01-01'
        AND '2022-12-31'
    """

    return db.raw_sql(query, date_cols=["date"])