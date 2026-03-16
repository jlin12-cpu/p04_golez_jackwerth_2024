# Dataframe: `P04:clean_options` - 

Cleaned SPX options dataset used for implied-rate and strip-price estimation. Adds the S&P 500 index level, applies minimum bid and ask filters, enforces a minimum maturity of 90 days, and restricts moneyness to the interval [0.5, 1.5].


## DataFrame Glimpse

```
Rows: 625615
Columns: 17
$ date             <datetime[ns]> 2024-12-31 00:00:00
$ exdate           <datetime[ns]> 2029-12-21 00:00:00
$ cp_flag                   <str> 'P'
$ strike                    <f64> 8800.0
$ best_bid                  <f64> 1642.4
$ best_offer                <f64> 1685.9
$ impl_volatility           <f64> 0.133519
$ delta                     <f64> -0.695044
$ gamma                     <f64> 0.00018
$ vega                      <f64> 4128.391
$ volume                    <f64> 0.0
$ open_interest             <f64> 1.0
$ mid_price                 <f64> 1664.15
$ days_to_maturity          <i64> 1816
$ year                      <i64> 2024
$ spindx                    <f64> 5881.63
$ moneyness                 <f64> 1.4961838809989747


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [clean_options](../dataframes/P04/clean_options.md)                                       |
| Data Sources                   | Derived from optionmetrics_spx_monthly and crsp_sp500_daily                                        |
| Data Providers                 | Internal pipeline                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Constructed by clean_data.py.                                                    |
| Data available up to (min)     | 2024-12-31 00:00:00                                                             |
| Data available up to (max)     | 2024-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full_stack/p04_golez_jackwerth_2024/_data/clean_options.parquet                                                   |


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


