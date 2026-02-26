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

## Usage

1. **Update data**: `python data_fetcher.py`
2. **Run dashboard**: `python app.py`
3. **View**: http://localhost:5002

## Limitations

- Excel max rows per sheet: 1,048,576
- FRED/BLS have no single "list all series" endpoint
- EIA uses route-tree not flat list
- BEA series are compound keys (dataset+table+line+freq)
- Full data universes may exceed storage limits - use Parquet/SQLite for full dumps
