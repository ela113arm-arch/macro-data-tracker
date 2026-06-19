# COT Long/Short Data

This repo includes a standalone fetcher for managed-money long/short positioning in WTI CME and ICE Brent futures.

## Run

```bash
python scripts/fetch_cot_long_short.py --start 2020-01-01
```

## Output

```text
data/cot_wti_brent_long_short.csv
```

## Main columns

- `WTI_CME_mm_long`, `WTI_CME_mm_short`, `WTI_CME_mm_net`
- `BRENT_ICE_mm_long`, `BRENT_ICE_mm_short`, `BRENT_ICE_mm_net`
- `COMBINED_mm_long`, `COMBINED_mm_short`, `COMBINED_mm_net`
- `*_ww` columns for week-over-week changes
- `*_open_interest` and `*_pct_oi` columns where available
- Optional `brent_close`, `wti_close`, `brent_ww`, and `wti_ww` price overlays from Yahoo Finance

Positions are converted from contracts to million barrels using 1,000 bbl per futures contract.

## Source logic

- WTI CME uses the CFTC Disaggregated Futures Only Socrata API with CFTC contract market code `067651`.
- Brent uses ICE yearly COT history CSV files named `COTHist{year}.csv`.
- Both are filtered to `FutOnly` managed-money long/short rows.

## Automation

The GitHub Actions workflow `.github/workflows/refresh-cot-long-short.yml` runs every Friday evening UTC and can also be manually triggered with `workflow_dispatch`. It writes and commits `data/cot_wti_brent_long_short.csv` only when the CSV changes.
