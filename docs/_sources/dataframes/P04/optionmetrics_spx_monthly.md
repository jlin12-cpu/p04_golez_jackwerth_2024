# Dataframe: `P04:optionmetrics_spx_monthly` - 

SPX options filtered to the last trading day of each month. This is the main raw options input used in the cleaning step and ultimately in implied-rate and strip-price estimation.


## DataFrame Glimpse

```
Rows: 760830
Columns: 16
$ date              <datetime[ns]> 2025-08-29 00:00:00
$ exdate            <datetime[ns]> 2030-12-20 00:00:00
$ cp_flag                    <str> 'P'
$ strike                     <f64> 12000.0
$ best_bid                   <f64> 3475.0
$ best_offer                 <f64> 3775.0
$ impl_volatility            <f64> 0.120326
$ delta                      <f64> -0.893851
$ gamma                      <f64> 6.8e-05
$ vega                       <f64> 1816.845
$ volume                     <f64> 0.0
$ open_interest              <f64> 4.0
$ mid_price                  <f64> 3625.0
$ days_to_maturity           <i64> 1939
$ year                       <i64> 2025
$ __index_level_0__          <i64> 15911562


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [optionmetrics_spx_monthly](../dataframes/P04/optionmetrics_spx_monthly.md)                                       |
| Data Sources                   | OptionMetrics IvyDB                                        |
| Data Providers                 | WRDS                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Constructed in pull_spx_options_and_zero_coupon.py by filtering raw SPX options to the month-end date within each calendar month.                                                    |
| Data available up to (min)     | 2025-08-29 00:00:00                                                             |
| Data available up to (max)     | 2025-08-29 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full_stack/p04_golez_jackwerth_2024/_data/optionmetrics_spx_monthly.parquet                                                   |


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


