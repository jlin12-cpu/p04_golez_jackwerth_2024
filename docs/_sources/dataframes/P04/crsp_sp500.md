# Dataframe: `P04:crsp_sp500` - 

Daily S&P 500 value-weighted return index data from CRSP. Includes returns with and without dividends (vwretd and vwretx) used to calculate realized dividends.


## DataFrame Glimpse

```
Rows: 7300
Columns: 4
$ date   <datetime[ns]> 2024-12-31 00:00:00
$ vwretd          <f64> -0.003392
$ vwretx          <f64> -0.003541
$ sprtrn          <f64> -0.004285


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [crsp_sp500](../dataframes/P04/crsp_sp500.md)                                       |
| Data Sources                   | CRSP                                        |
| Data Providers                 | WRDS                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Downloaded via WRDS API using wrds-python library. Query pulls daily returns from crsp.dsi table including vwretd (with dividends) and vwretx (ex-dividends).                                                    |
| Data available up to (min)     | 2024-12-31 00:00:00                                                             |
| Data available up to (max)     | 2024-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full stack/p04_golez_jackwerth_2024/_data/crsp_sp500_daily.parquet                                                   |


**Linked Charts:**


- [P04:sp500_cumulative](../../charts/P04.sp500_cumulative.md)

- [P04:sp500_volatility](../../charts/P04.sp500_volatility.md)



## Pipeline Manifest

| Pipeline Name                   | Holding Period Effects in Dividend Strip Returns                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [P04](../index.md)              |
| Lead Pipeline Developer         | Jie Lin and Zimeng Yi             |
| Contributors                    | Jie Lin and Zimeng Yi           |
| Git Repo URL                    |                         |
| Pipeline Web Page               | <a href="file:///Users/jielin/Desktop/full stack/p04_golez_jackwerth_2024/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-02-10 02:02:29           |
| OS Compatibility                |  |
| Linked Dataframes               |  [P04:crsp_sp500](../dataframes/P04/crsp_sp500.md)<br>  [P04:fred_treasury_rates](../dataframes/P04/fred_treasury_rates.md)<br>  [P04:crsp_treasury_returns](../dataframes/P04/crsp_treasury_returns.md)<br>  [P04:optionmetrics_spx](../dataframes/P04/optionmetrics_spx.md)<br>  [P04:fama_french_factors](../dataframes/P04/fama_french_factors.md)<br>  |


