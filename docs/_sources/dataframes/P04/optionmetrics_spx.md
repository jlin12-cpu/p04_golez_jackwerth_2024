# Dataframe: `P04:optionmetrics_spx` - 

European S&P 500 index options from OptionMetrics IvyDB. Data filtered to last business day of each month, with maturity >= 90 days and bid-ask midpoint >= $3. Includes calls, puts, strikes, implied volatility, and Greeks.


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
| Dataframe ID                   | [optionmetrics_spx](../dataframes/P04/optionmetrics_spx.md)                                       |
| Data Sources                   | OptionMetrics IvyDB                                        |
| Data Providers                 | WRDS                                      |
| Links to Providers             |                              |
| Topic Tags                     |                                           |
| Type of Data Access            |                                   |
| How is data pulled?            | Downloaded year-by-year from OptionMetrics via WRDS. Filtered for: (1) SPX European options (secid=108105), (2) last business day of each month, (3) maturity >= 90 days, (4) bid-ask midpoint >= $3. Includes call and put options with strikes, implied volatility, and Greeks.                                                    |
| Data available up to (min)     | 2025-08-29 00:00:00                                                             |
| Data available up to (max)     | 2025-08-29 00:00:00                                                             |
| Dataframe Path                 | /Users/jielin/Desktop/full stack/p04_golez_jackwerth_2024/_data/optionmetrics_spx_monthly.parquet                                                   |


**Linked Charts:**


- [P04:options_count](../../charts/P04.options_count.md)

- [P04:options_prices](../../charts/P04.options_prices.md)



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


