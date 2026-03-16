# Dataframe: `P04:fama_french_monthly` - 

Monthly Fama-French factor data in decimal form. The 1-month risk-free rate from this file is used in Table 1 and in Figures 2–3 when constructing excess returns over the risk-free rate.


## DataFrame Glimpse

```
Rows: 360
Columns: 5
$ date           <datetime[ns]> 2025-12-31 00:00:00
$ mkt_rf_monthly          <f64> -0.0036
$ smb_monthly             <f64> -0.0106
$ hml_monthly             <f64> 0.0242
$ rf_1m_monthly           <f64> 0.0034000000000000002


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [fama_french_monthly](../dataframes/P04/fama_french_monthly.md)                                       |
| Data Sources                   | Fama-French Data Library                                        |
| Data Providers                 | Kenneth French                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Downloaded using pandas_datareader in pull_fred.py.                                                    |
| Data available up to (min)     | 2025-12-31 00:00:00                                                             |
| Data available up to (max)     | 2025-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full_stack/p04_golez_jackwerth_2024/_data/fama_french_monthly.parquet                                                   |


**Linked Charts:**

- None


## Pipeline Manifest

| Pipeline Name                   | Holding Period Effects in Dividend Strip Returns                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [P04](../index.md)              |
| Lead Pipeline Developer         | Jie Lin and Zimeng Yi             |
| Contributors                    | Jie Lin, Zimeng Yi           |
| Git Repo URL                    |                         |
| Pipeline Web Page               | <a href="file:///Users/jielin/Desktop/full_stack/p04_golez_jackwerth_2024/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-03-16 00:21:10           |
| OS Compatibility                |  |
| Linked Dataframes               |  [P04:crsp_sp500_daily](../dataframes/P04/crsp_sp500_daily.md)<br>  [P04:crsp_treasury_returns](../dataframes/P04/crsp_treasury_returns.md)<br>  [P04:fred_treasury_rates](../dataframes/P04/fred_treasury_rates.md)<br>  [P04:fama_french_factors](../dataframes/P04/fama_french_factors.md)<br>  [P04:fama_french_monthly](../dataframes/P04/fama_french_monthly.md)<br>  [P04:optionmetrics_spx_raw](../dataframes/P04/optionmetrics_spx_raw.md)<br>  [P04:optionmetrics_spx_monthly](../dataframes/P04/optionmetrics_spx_monthly.md)<br>  [P04:optionmetrics_zero_curve](../dataframes/P04/optionmetrics_zero_curve.md)<br>  [P04:clean_options](../dataframes/P04/clean_options.md)<br>  [P04:clean_zero_curve](../dataframes/P04/clean_zero_curve.md)<br>  [P04:clean_rates](../dataframes/P04/clean_rates.md)<br>  [P04:clean_crsp_sp500_monthly](../dataframes/P04/clean_crsp_sp500_monthly.md)<br>  [P04:implied_rates](../dataframes/P04/implied_rates.md)<br>  [P04:implied_rates_1y](../dataframes/P04/implied_rates_1y.md)<br>  [P04:zero_curve_1y](../dataframes/P04/zero_curve_1y.md)<br>  [P04:strip_prices](../dataframes/P04/strip_prices.md)<br>  [P04:all_strip_prices](../dataframes/P04/all_strip_prices.md)<br>  [P04:monthly_returns](../dataframes/P04/monthly_returns.md)<br>  |


