# Dataframe: `P04:monthly_returns` - 

Key derived panel used for Table 1 and Figures 2–3. Contains strip and market log returns, the monthly risk-free rate, Treasury return adjustments, and excess-return variants under both risk-free and Treasury benchmarks.


## DataFrame Glimpse

```
Rows: 348
Columns: 10
$ date          <datetime[ns]> 2024-12-31 00:00:00
$ strip_ret              <f64> 0.4327664481665225
$ mkt_ret                <f64> -0.03212699599105942
$ rf_1m_monthly          <f64> 0.0037
$ treas_2y_log           <f64> 0.001951095374918778
$ treas_10y_log          <f64> -0.02811967827737156
$ strip_ret_rf           <f64> 0.4290664481665225
$ mkt_ret_rf             <f64> -0.03582699599105942
$ strip_ret_2y           <f64> 0.4308153527916037
$ mkt_ret_10y            <f64> -0.004007317713687862


```

## Dataframe Manifest

| Dataframe Name                 |                                                    |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [monthly_returns](../dataframes/P04/monthly_returns.md)                                       |
| Data Sources                   | Derived from strip_prices, all_strip_prices, clean_crsp_sp500_monthly, fama_french_monthly, and crsp_treasury_returns                                        |
| Data Providers                 | Internal pipeline                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Constructed by calc_returns.py.                                                    |
| Data available up to (min)     | 2024-12-31 00:00:00                                                             |
| Data available up to (max)     | 2024-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full_stack/p04_golez_jackwerth_2024/_data/calc/monthly_returns.parquet                                                   |


**Linked Charts:**


- [P04:figure2](../../charts/P04.figure2.md)

- [P04:figure2_extension](../../charts/P04.figure2_extension.md)

- [P04:figure2_extension_winsorized](../../charts/P04.figure2_extension_winsorized.md)

- [P04:figure3](../../charts/P04.figure3.md)

- [P04:figure3_extension](../../charts/P04.figure3_extension.md)



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


