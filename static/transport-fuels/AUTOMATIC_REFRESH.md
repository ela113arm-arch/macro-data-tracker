# Automatic transport-fuel refresh

The existing `.github/workflows/refresh-data.yml` runs the macro refresh on
weekdays at 14:00, 16:00 and 23:30 UTC, and supports `workflow_dispatch`.
During each run, `data_fetcher.py` calls
`scripts/transport_fuels_fetcher.py`, which retrieves all ten official
publisher series and passes a complete manifest to
`scripts/update_transport_fuels.py`. Render deploys the resulting `master`
branch; the Willowdesk wrapper only embeds the Render dashboard.

Each adapter retrieves the complete July 2020 through latest complete month
history, checks source labels, geography, units, and product mapping, and
writes a normalized temporary CSV. July 2020 is required for the July 2021
year-over-year comparison. Monthly source levels are converted using actual
calendar days before the year-over-year calculation. No estimates, zeros,
third-party substitutes, or back-solved history are allowed.

The manifest always contains all ten country names. A failed source records an
error with the country and stage; the publisher retains that country's prior
data, metadata, source-level CSV, and last-success timestamp while publishing
any valid countries. New months and revisions are recomputed from the full
source history. The publisher also records retrieval vintage, validation note,
and normalized-source checksum.

To run the ETL locally:

```sh
python scripts/transport_fuels_fetcher.py
python -m unittest discover -s tests -p 'test_transport_fuels*.py'
node --check static/transport-fuels/charts.js
```

A partial source failure is expected to update `refresh-status.json` and still
commit valid output files. Inspect the logged country results before any
manual rerun. The UI treats a successful check older than 96 hours as overdue,
which allows for the normal weekend gap in the weekday schedule.
