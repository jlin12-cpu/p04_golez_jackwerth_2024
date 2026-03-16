# Dataframe: `P04:optionmetrics_zero_curve` - 

Daily zero-coupon term structure from OptionMetrics, with maturity in days and associated continuously compounded rates. Used as the zero-curve benchmark in Figure 1.


## DataFrame Glimpse

```
Rows: 304301
Columns: 3
$ date <datetime[ns]> 2025-08-29 00:00:00
$ days          <f64> 730.0
$ rate          <f64> 3.890997


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [optionmetrics_zero_curve](../dataframes/P04/optionmetrics_zero_curve.md)                                       |
| Data Sources                   | OptionMetrics IvyDB                                        |
| Data Providers                 | WRDS                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Downloaded from optionm.zerocd via WRDS using pull_spx_options_and_zero_coupon.py.                                                    |
| Data available up to (min)     | 2025-08-29 00:00:00                                                             |
| Data available up to (max)     | 2025-08-29 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full_stack/p04_golez_jackwerth_2024/_data/optionmetrics_zero_curve.parquet                                                   |


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


