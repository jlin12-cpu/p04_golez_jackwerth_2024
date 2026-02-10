# Dataframe: `P04:crsp_treasury_returns` - 

Monthly total returns on 2-year and 10-year constant maturity Treasury bonds from CRSP. Returns include both price changes and coupon income.


## DataFrame Glimpse

```
Rows: 348
Columns: 3
$ date             <datetime[ns]> 2024-12-31 00:00:00
$ treasury_2y_ret           <f64> 0.001953
$ treasury_10y_ret          <f64> -0.027728


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [crsp_treasury_returns](../dataframes/P04/crsp_treasury_returns.md)                                       |
| Data Sources                   | CRSP                                        |
| Data Providers                 | WRDS                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Downloaded via WRDS API from crsp.mcti table (Monthly Constant Maturity Treasury Index). Returns are total returns including price changes and coupon income.                                                    |
| Data available up to (min)     | 2024-12-31 00:00:00                                                             |
| Data available up to (max)     | 2024-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full stack/p04_golez_jackwerth_2024/_data/crsp_treasury_returns.parquet                                                   |


**Linked Charts:**


- [P04:treasury_returns](../../charts/P04.treasury_returns.md)



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


