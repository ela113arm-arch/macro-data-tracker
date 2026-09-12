# Automatic transport-fuel refresh

The enabled ChatGPT task **Refresh transport fuels** checks all ten official
publishers daily and updates the public `ela113arm-arch/macro-data-tracker`
repository, branch `master`. This is an agent-run source refresh, not a reminder
and not part of `data_fetcher.py`. Render automatically publishes branch changes;
Willowdesk embeds that dashboard. Do not modify or redeploy the ChatGPT Site wrapper.

## One refresh iteration

1. Fetch current `master`. Read `REPRODUCTION_GUIDE.md`, the current CSV catalog,
   `data.json`, `refresh-status.json`, and `scripts/update_transport_fuels.py`.
2. Retrieve official source data for every country. Discover the latest download
   through the official landing page; the dated example URLs in the guide are not
   permanent endpoints. Roll fiscal/calendar years and Petronet end dates forward.
   Check historical revisions as well as newly published months. Preserve the
   existing product definitions, units and national geography. Do not silently
   substitute JODI, retail sales, weekly estimates or broader/narrower products.
3. For each successful source, save a normalized CSV with these columns:
   `date,gasoline,jet_fuel,diesel,native_unit`. Include a continuous, unique monthly
   history from July 2020 through the latest complete officially published month.
   Use actual source levels, not back-solved values from the existing chart. Keep
   blank/unpublished observations missing; never turn them into zeros or estimates.
   Verify the final month's three values against the original table. An HTTP 200
   or a newer page date is not a successful data check.
4. Prepare one JSON manifest with all ten country names as keys. Success example:

   ```json
   {
     "United States": {
       "status": "ok",
       "csv": "united_states.csv",
       "source_urls": ["https://www.eia.gov/dnav/pet/xls/PET_CONS_PSUP_DC_NUS_MBBLPD_M.xls"],
       "vintage": "Actual release date and data cutoff read from the source",
       "validation_note": "Record the final month and three native values checked against the official table."
     }
   }
   ```

   This fragment illustrates one country; the actual manifest must include all ten.
   On an inaccessible or unparseable source, supply
   `{"status":"error","message":"Specific failure; previous data retained."}`.
   Do not mark success until the data was retrieved and validated. Do not infer
   that the source has no new data because a guessed download URL returned 404.
5. Run `python scripts/update_transport_fuels.py /absolute/path/to/manifest.json`.
   It standardizes calendar-day rates, recalculates YoY, updates the chart JSON,
   master CSV and catalog together, saves normalized source levels with checksum
   provenance, and records country health independently. It rejects gaps,
   duplicates, missing/nonfinite values, unit mismatches, gross scale changes,
   future months and truncation of the current chart history. Exit 1 means a
   partial source failure: valid countries and error statuses are still written.
6. Run `python -m unittest discover -s tests -p 'test_transport_fuels*.py'` and
   `node --check static/transport-fuels/charts.js`. Inspect the changed cutoff
   dates and unusual revisions before publishing. Preserve all failed countries'
   data and last-success timestamps. If extraction fails, adapt only the relevant
   official-source parser and retry once; report unresolved failures.
7. Commit only the fuel data/status/source-level files (and any needed focused
   extraction fix) to the existing `master` branch. Fetch/rebase if another job
   changed the branch; never force-push or overwrite unrelated work. Native GitHub
   create-tree/create-commit/update-ref tools are available when shell Git lacks
   write authentication. Use the current remote parent, not a previous run's SHA.
8. Verify the live `/macro` page and `/static/transport-fuels/data.json` plus the
   CSV at `https://macro-data-tracker.onrender.com` match the intended update after
   Render finishes. A push alone is not proof of publication. If publication is
   delayed or fails, report it accurately; do not claim the site is updated.

## Source discovery reminders

- US: EIA monthly product supplied MGFUPUS2 / MKJUPUS2 / MDIUPUS2.
- India: PPAC products-wise page and its public consumption service; April–March
  financial years; MS / ATF / HSD in TMT.
- Brazil: ANP open fuel-sales dataset; discover current filename instead of fixing
  the ending year; sum states once for GASOLINA C / aviation kerosene / diesel.
- Japan: METI petroleum statistics, monthly domestic sales; record preliminary vs
  final; gasoline / jet / gas oil in kilolitres.
- Mexico: PEMEX RVOLVIN internal sales; verify the displayed unit before converting.
- UK: discover current Table 3.13 from
  https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends
  rather than reusing the August 2026 asset; petrol / jet / white plus red diesel.
- Spain: discover current and historical CORES consumption workbooks; include a
  new annual workbook after year-end; total gasoline / kerosene / gasoil.
- Australia: APS current data extract, national monthly `Sales of products`.
- South Korea: KNOC Petronet KDCQ0200 consumption; extend end date through the
  current completed month; confirm product mapping on every changed layout.
- Canada: Statistics Canada 25-10-0081-01, Canada, `Products supplied, disposition`.

The UI shows the data cutoff and successful verification time separately. After
48 hours without a successful check it marks the source check overdue, even if
no task ran to record a failure. Pending is not a completed source verification.
Country charts retain independent data cutoffs and automatically extend their
horizontal axes. Never sum mixed latest months into a purported global total.

Notify the owner in ChatGPT about new months, material revisions, source failures,
or publication problems. Stay quiet when all sources are unchanged and healthy.
Do not send email or Slack messages or use paid sources for this task.
