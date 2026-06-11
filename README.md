# Macro Data Tracker

A Flask dashboard for tracking US macroeconomic data from official government APIs.

## Architecture

```
macro_data_tracker/
├── app.py              # Flask server (reads from CSV)
├── data_fetcher.py     # Data pull module (saves to CSV)
├── config/
│   └── api_keys.py     # API keys storage
├── data/               # CSV data files
│   ├── gdp_components.csv
│   ├── gdp_contributions.csv
│   ├── trade_detail.csv
│   ├── import_categories.csv
│   ├── export_categories.csv
│   ├── commodities.csv
│   └── metadata.csv
└── templates/
    └── index.html      # Dashboard UI
```

## Data Sources

| Source | Description | API Base |
|--------|-------------|----------|
| FRED | Federal Reserve Economic Data | https://api.stlouisfed.org/fred |
| BEA | Bureau of Economic Analysis | https://apps.bea.gov/api |
| BLS | Bureau of Labor Statistics | https://api.bls.gov/publicAPI |
| EIA | Energy Information Administration | https://api.eia.gov/v2 |
| CFTC | Commitment of Traders Disaggregated Futures Only | https://publicreporting.cftc.gov/resource/72hh-3qpy.json |
| ICE | Brent futures historical COT CSVs | https://www.ice.com/publicdocs/futures |
| Yahoo Finance | Futures and market prices via yfinance | N/A |
| Robbie Andrew vehicle registrations | Global monthly vehicle registrations by fuel type | https://robbieandrew.github.io/carsales/ |

## API Enumeration Guide

### FRED (Federal Reserve Economic Data)
- **Enumerate via releases**: `GET /fred/releases` to list releases, then `/fred/release/series?release_id=...` to list series per release
- **Fetch series metadata**: `GET /fred/series?series_id=...` returns title, units, frequency, seasonal_adjustment, dates, popularity, notes
- **Add categories/tags**: `GET /fred/series/categories` and `/fred/series/tags` per series
- **Note**: No single "list all series" endpoint - must enumerate via releases/search

### BEA (Bureau of Economic Analysis)
- **Enumerate datasets**: `GET DataSetList` (method=GetDataSetList)
- **Get parameters**: For each dataset, pull ParameterList, then enumerate parameter values (GetParameterValues)
- **Series identity**: BEA 'series' is compound of datasetName + TableID + LineNumber (or SeriesCode) + Frequency
- **NIPA Tables**: National Income and Product Accounts (GDP, consumption, investment, trade)

### BLS (Bureau of Labor Statistics)
- **Hard truth**: No official "list every series id" endpoint in public API
- **What you can do**: Use `/surveys` to list surveys; `/timeseries/popular?survey=XX` for popular series
- **Full universe**: Need bulk series list files from BLS website

### EIA (Energy Information Administration)
- **Route-tree structure**: Not flat list - traverse `/v2/` routes recursively
- **Enumerate routes**: Start at `https://api.eia.gov/v2/`, traverse route tree until leaf datasets
- **Fetch metadata**: `GET {base}/v2/{route_path}?api_key=...` (no /data)

## Standard Schema for Series Metadata

| Column | Meaning |
|--------|---------|
| source | Agency source: EIA / BEA / BLS / FRED |
| series_key | Primary key for requesting data |
| title | Short human-readable title |
| description | Long description/notes |
| units | Full unit description |
| units_short | Short unit label |
| frequency | Frequency label (Monthly/Quarterly/etc) |
| frequency_code | Frequency short code |
| seasonal_adjustment | SA label if available |
| seasonal_adjustment_code | SA short code |

## Current Data Series

### GDP Contributions (FRED)
| Series ID | Description |
|-----------|-------------|
| DGDSRY2Q224SBEA | PCE Goods contribution |
| DSERRY2Q224SBEA | PCE Services contribution |
| A008RY2Q224SBEA | Nonresidential Investment contribution |
| A011RY2Q224SBEA | Residential Investment contribution |
| A014RY2Q224SBEA | Inventories contribution |
| A019RY2Q224SBEA | Net Exports contribution |
| A822RY2Q224SBEA | Government contribution |
| A191RL1Q225SBEA | Total Real GDP Growth |

### Trade Detail (FRED/BEA)
| Series ID | Description |
|-----------|-------------|
| A253RC1Q027SBEA | Exports of Goods |
| A646RC1Q027SBEA | Exports of Services |
| A255RC1Q027SBEA | Imports of Goods |
| B656RC1Q027SBEA | Imports of Services |

### Import Categories (FRED/BEA)
| Series ID | Description |
|-----------|-------------|
| A650RC1Q027SBEA | Capital Goods Imports |
| A652RC1Q027SBEA | Consumer Goods Imports |
| B651RC1Q027SBEA | Automotive Imports |
| B647RC1Q027SBEA | Foods Imports |
| LA0000041Q027SBEA | Industrial Supplies Imports |
| B648RC1Q027SBEA | Petroleum Imports |

### Export Categories (FRED/BEA)
| Series ID | Description |
|-----------|-------------|
| A640RC1Q027SBEA | Capital Goods Exports |
| A642RC1Q027SBEA | Consumer Goods Exports |
| B641RC1Q027SBEA | Automotive Exports |
| B181RC1Q027SBEA | Agricultural/Foods Exports |
| A639RC1Q027SBEA | Industrial Supplies Exports |
| LA0000061Q027SBEA | Petroleum Exports |

The Trade by Category dashboard renders exports and imports as quarterly seasonal charts by category. Each chart uses Q1-Q4 on the x-axis, a five-year historical range band, a five-year average, prior year, and current year.

The Detailed Trade dashboard uses the same quarterly seasonal chart pattern for granular BEA ITA categories such as pharmaceuticals, gold, precious metals, semiconductors, telecom equipment, chemicals, steel, and apparel. Current local data coverage is through 2025Q3 for `trade_categories.csv` and 2025Q4 for `detailed_trade.csv`; no 2026 observations are present in the current refreshed CSVs.

### COT Energy Positioning (CFTC/ICE/Yahoo Finance)
| Field | Description |
|-------|-------------|
| `BRENT_ICE_mm_net` | Brent ICE managed-money net position in million barrels |
| `WTI_CME_mm_net` | WTI CME managed-money net position in million barrels |
| `COMBINED_mm_net` | Brent plus WTI managed-money net position in million barrels |
| `COMBINED_mm_net_ww` | Week-over-week change in combined managed-money net position |
| `brent_close` | Brent Tuesday-close futures price |
| `brent_ww` | Week-over-week change in Brent Tuesday-close price |
| `wti_close` | WTI Tuesday-close futures price |
| `wti_ww` | Week-over-week change in WTI Tuesday-close price |

The COT dashboard uses Tuesday report dates because COT data is recorded as of Tuesday. The "Managed Money Positioning Change vs Brent Change" chart plots `COMBINED_mm_net_ww` against `brent_ww`, overlays an OLS trend line, highlights the last five observations, and displays R-squared, correlation, beta, observation count, and latest Brent weekly change.

The exploratory relationship table compares weekly COT/Brent observations with Energy ETF volume pressure. ETF pressure is calculated as daily volume divided by its 50-day moving average minus one, averaged over the Tuesday-ending week for each COT report date.

### Total Inventory EIA WTI Fair Value (EIA/Yahoo/FRED)
| Field | Description |
|-------|-------------|
| `total_stocks` | EIA `WTESTUS1` weekly U.S. ending stocks excluding SPR of crude oil and petroleum products, in MMbbl |
| `ntps` | Normalized Total Petroleum Stocks: `total_stocks` minus the same ISO-week average over 2011-2018 |
| `wti_nominal` | Yahoo Finance `CL=F` Friday weekly close |
| `wti_real` | WTI adjusted to latest CPI dollars using FRED `CPIAUCSL` |
| `fair_value` | Hyperbolic fair value fit on 2012+ observations |
| `fair_value_plus_1sigma` / `fair_value_minus_1sigma` | One residual standard deviation around fair value |
| `fair_value_plus_2sigma` / `fair_value_minus_2sigma` | Two residual standard deviations around fair value |
| `residual_z` | Real WTI minus fair value, divided by residual sigma |

The Total Inventory EIA chart is available on the root modeling dashboard and the macro Energy dashboard. Both dashboards also include a week-over-week change view that plots `diff(ntps)` against `diff(wti_real)`, highlights the latest observation and trailing 13 weeks, and displays correlation, R-squared, and beta. The standard refresh button runs the total-inventory fetcher, updating `eia_total_stocks.csv`, `wti_prices.csv`, `cpi_monthly.csv`, and `total_inv_eia_fair_value.csv`.

### EV Fleet Dashboard (Robbie Andrew/Yahoo)
| File | Description |
|------|-------------|
| `ev_carsales_monthly_raw.csv` | Raw all-country monthly registrations by country and fuel type |
| `ev_brent_monthly.csv` | Monthly Brent futures closes and monthly/year-over-year changes |
| `ev_monthly_fleet.csv` | Processed monthly ICE/EV sales, cumulative fleet, fleet share, and Brent overlay |
| `ev_annual_fleet.csv` | Annual rollup of sales and cumulative fleet by country |
| `ev_policy_events.csv` | China, USA, and India policy markers used in the EV chart annotations |

The EV pipeline classifies BEV, PHEV, and ZEV as EV; petrol, diesel, LPG, ethanol blends, hydrogen, and other fuels as ICE; and splits non-plugin, mild, and generic hybrids 50/50 between EV and ICE. Fleet stock is the cumulative sum of monthly registrations, so it is an upper-bound stock series and does not subtract vehicle retirements, exports, or scrappage. The standard `python data_fetcher.py` refresh and the GitHub Actions scheduled refresh both rebuild these EV CSVs.

## Usage

1. **Update data**: `python data_fetcher.py`
2. **Run dashboard**: `python app.py`
3. **View**: http://localhost:5003

`python data_fetcher.py` refreshes the macro, energy, SPR-adjacent, and EV fleet CSVs. The EV section is available at `/macro?section=ev` after the CSVs are present.

## Deploy

The app is ready to deploy from GitHub using the included `Dockerfile`, `Procfile`, and `render.yaml`. Set `FRED_API_KEY`, `BEA_API_KEY`, and `EIA_API_KEY` as host environment variables; never commit real keys. The `Refresh data` GitHub Actions workflow runs `python data_fetcher.py` on its weekday cron schedule, so committed refreshes include the EV fleet CSVs as well as the existing macro and energy data. Render picks up committed CSV changes after a redeploy, and `/api/status` reports whether an external `DATA_DIR` is masking the bundled GitHub data. See `DEPLOYMENT.md` for the full setup.

## Limitations

- Excel max rows per sheet: 1,048,576
- FRED/BLS have no single "list all series" endpoint
- EIA uses route-tree not flat list
- BEA series are compound keys (dataset+table+line+freq)
- ICE COT data is downloaded from yearly CSV files and depends on ICE column naming remaining stable
- COT/price comparisons use Tuesday alignment; Friday closes should not be mixed with COT reporting dates
- Full data universes may exceed storage limits - use Parquet/SQLite for full dumps
