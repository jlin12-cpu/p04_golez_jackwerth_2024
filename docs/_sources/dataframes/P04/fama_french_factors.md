# Dataframe: `P04:fama_french_factors` - 

Daily Fama-French three-factor model data including market factor (Mkt-RF), size factor (SMB), value factor (HML), and risk-free rate. All factors in decimal form.


## DataFrame Glimpse

```
Rows: 7550
Columns: 5
$ date   <datetime[ns]> 2025-12-31 00:00:00
$ mkt_rf          <f64> -0.0076
$ smb             <f64> 0.0007000000000000001
$ hml             <f64> -0.0009
$ rf_1m           <f64> 0.0002


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [fama_french_factors](../dataframes/P04/fama_french_factors.md)                                       |
| Data Sources                   | Fama-French Data Library                                        |
| Data Providers                 | Kenneth French                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Downloaded via pandas_datareader from Kenneth French Data Library. Contains daily market factor (Mkt-RF), size factor (SMB), value factor (HML), and risk-free rate.                                                    |
| Data available up to (min)     | 2025-12-31 00:00:00                                                             |
| Data available up to (max)     | 2025-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full stack/p04_golez_jackwerth_2024/_data/fama_french_factors.parquet                                                   |


**Linked Charts:**

- None


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


