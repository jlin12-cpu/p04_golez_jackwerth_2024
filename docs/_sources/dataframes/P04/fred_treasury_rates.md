# Dataframe: `P04:fred_treasury_rates` - 

Daily Treasury rates from multiple sources. 1-month T-bill rate from Fama-French data library (annualized from daily rate), 2-year and 10-year constant maturity rates from FRED.


## DataFrame Glimpse

```
Rows: 7828
Columns: 4
$ date         <datetime[ns]> 2025-12-31 00:00:00
$ rf_1m                 <f64> 0.0002
$ treasury_2y           <f64> 0.0347
$ treasury_10y          <f64> 0.0418


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [fred_treasury_rates](../dataframes/P04/fred_treasury_rates.md)                                       |
| Data Sources                   | Fama-French Data Library, FRED                                        |
| Data Providers                 | Kenneth French, Federal Reserve                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | 1-month T-bill pulled from Fama-French daily factors. 2-year and 10-year Treasury rates pulled from FRED using pandas_datareader. RF rate annualized by multiplying daily rate by 252.                                                    |
| Data available up to (min)     | 2025-12-31 00:00:00                                                             |
| Data available up to (max)     | 2025-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full stack/p04_golez_jackwerth_2024/_data/fred_treasury_rates.parquet                                                   |


**Linked Charts:**


- [P04:treasury_rates](../../charts/P04.treasury_rates.md)



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


