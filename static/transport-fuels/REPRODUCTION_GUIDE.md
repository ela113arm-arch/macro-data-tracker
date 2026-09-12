# International Transport-Fuel Chart Reconstruction Playbook

## Purpose and final standard

Use this playbook to rebuild the monthly transport-fuel country charts from first principles. The package covers **United States, India, Brazil, Japan, Mexico, United Kingdom, Spain, Australia, South Korea, and Canada**. Every country chart must begin in **July 2021**, use the same colors and labels, report all results in **calendar-day-average million barrels per day (M b/d)**, and extend to that country’s latest verified official month. The first required source-level date is **July 2020** because July 2021 needs a same-month-prior-year comparison.

The colored bars are the product-level year-over-year contribution. Calculate each as the converted M b/d rate in month *t* less the rate in month *t − 12*. Stack positive contributions above zero and negative contributions below zero separately. The black line is the sum of gasoline, jet fuel, and diesel contributions. It is **not** total oil demand.

## Standard input and output contract

Save one clean source-level CSV per country before converting anything. The minimum source-level schema is:

```text
date,gasoline_level,jet_fuel_level,diesel_level,native_unit,source_vintage
```

Dates are ISO first-of-month dates. Use a continuous monthly range from July 2020 to the source’s latest published month. Do not pad a late country with zeros or estimate an unpublished month. Produce a standardized master table with:

```text
country,date,measure,native_unit,source,source_url,
gasoline_native,jet_fuel_native,diesel_native,
gasoline_mbd,jet_fuel_mbd,diesel_mbd,
gasoline_yoy_mbd,jet_fuel_yoy_mbd,diesel_yoy_mbd,net_yoy_mbd
```

## Official sources, pulls, mappings, and cleaning

| Country | Official publisher and source | Native measure and unit | Exact product mapping | Latest verified current vintage | Key extraction and cleaning rule |
|---|---|---|---|---|---|
| United States | U.S. Energy Information Administration (EIA), [monthly history workbook][1] and [Product Supplied page][2] | Product supplied; thousand barrels per day | `MGFUPUS2` Finished Motor Gasoline; `MKJUPUS2` Kerosene-Type Jet Fuel; `MDIUPUS2` Distillate Fuel Oil | PSM released 1 Sep. 2026; data through Jun. 2026 | Read `Data 1`; source keys are in the second row and dates begin after the title rows. Use U.S. national values only. Product supplied is a supply-chain proxy for consumption. |
| India | Petroleum Planning & Analysis Cell (PPAC), [Domestic Consumption of Petroleum Products][3] | Domestic market demand; thousand metric tonnes (TMT) | `MS` Motor Spirit; `ATF` Aviation Turbine Fuel; `HSD` High-Speed Diesel | PPAC service accessed 2 Sep. 2026; data through Jul. 2026 | POST to `https://ppac.gov.in/AjaxController/getConsumptionPetroleumProductsData` with `financialYear=YYYY-YYYY`, `reportBy=1`, and `pageId=43`. Convert April–March fiscal columns to calendar dates. Ignore only all-blank unpublished tail months. |
| Brazil | Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP), [fuel-sales CSV][4] | Distributor sales; cubic metres | `GASOLINA C`; `QUEROSENE DE AVIAÇÃO`; `ÓLEO DIESEL` | CSV updated 28 Aug. 2026; data through Jul. 2026 | Aggregate the 27 federative-unit source records for each product and reference month. Do not double-count a national aggregate, region subtotal, or monthly total row if supplied. Preserve decimal precision. |
| Japan | Agency for Natural Resources and Energy, Ministry of Economy, Trade and Industry (METI), [Petroleum Statistics][5] | Domestic sales by manufacturers and importers; kilolitres | Gasoline; Jet Fuel; Gas Oil | Current reports available through Jul. 2026; Jun. 2026 final workbook released 17 Aug. 2026 | Download the current historical workbook and prior workbooks as required. Extract monthly Domestic Sales, not production or inventory. Treat 1 kilolitre as 1 cubic metre. Record whether latest values are preliminary. |
| Mexico | Petróleos Mexicanos (PEMEX), [Institutional Database table RVOLVIN][6] | Internal sales volumes; cubic metres | Gasolinas; Turbosina; Diesel | Table accessed 2 Sep. 2026; data through Jul. 2026 | Use the table’s CSV export or official rendered response. Select national monthly `Volumen de ventas internas`; do not use production, exports, or price data. Validate current-month levels separately against the table. |
| United Kingdom | Department for Energy Security and Net Zero (DESNZ), [Energy Trends Table 3.13][7] | Deliveries for inland consumption; thousand tonnes | Petrol; Jet fuel; White diesel plus Red diesel (Gas oil) | August 2026 vintage; data through Jun. 2026 | Read monthly Table 3.13. Add White diesel and Red diesel into the diesel series before converting. Keep provisional flags and publication notes. |
| Spain | Corporación de Reservas Estratégicas de Productos Petrolíferos (CORES), [2026 consumption workbook][8] and annual workbooks for preceding years | Petroleum-product consumption; tonnes | Gasolinas total; Querosenos total; Gasóleos total | 2026 workbook updated 17 Aug. 2026; data through Jun. 2026 | Download annual workbooks from 2020 onward. Use the national total rows exactly; do not reconstruct them from subcategories. Flag workbook values as provisional. |
| Australia | Department of Climate Change, Energy, the Environment and Water (DCCEEW), [Australian Petroleum Statistics][9] | National petroleum-product sales; megalitres | Automotive gasoline; Aviation turbine fuel; Diesel oil total | June 2026 data extract published 14 Aug. 2026 | Download the current APS data extract. In the `Sales of products` worksheet, select national monthly levels. Retain the official diesel-oil-total label because it can be broader than automotive diesel. |
| South Korea | Korea National Oil Corporation (KNOC), [Petronet KDCQ0200 extract][10] | Domestic consumption; thousand barrels | Gasoline; Aviation fuel; Diesel | Petronet response retrieved 2 Sep. 2026; data through Jul. 2026 | Query the monthly KDCQ0200 extract for the full July 2020–latest span. Keep national domestic-consumption records only. Confirm that the product codes and columns remain stable before appending a refresh. |
| Canada | Statistics Canada, [Table 25-10-0081-01][11] | Products supplied, disposition; cubic metres | Finished motor gasoline; Kerosene-type jet fuel; Distillate fuel oil | Released 31 Aug. 2026; data through Jun. 2026 | Download the official CSV table, filter national geography and the `Products supplied, disposition` measure, then pivot the three product names. Do not substitute a retail-sales measure. |

## Product and unit conversion methodology

Convert the **monthly source level** to a calendar-day-average rate before calculating year-over-year differences. Use the actual number of days in each reference month. Do not calculate YoY from annualized or month-total differences unless the result is divided by the relevant days in both periods.

| Source unit | Formula for product-level M b/d |
|---|---|
| Thousand b/d | `level / 1,000` |
| Thousand barrels | `level × 1,000 / days_in_month / 1,000,000` |
| Cubic metres or kilolitres | `level × 6.28981077 / days_in_month / 1,000,000` |
| Megalitres | `level × 1,000 × 6.28981077 / days_in_month / 1,000,000` |
| Tonnes | `level × product_barrels_per_tonne / days_in_month / 1,000,000` |
| Thousand tonnes or TMT | `level × 1,000 × product_barrels_per_tonne / days_in_month / 1,000,000` |

For mass-based series, use the United Nations Statistics Division factors shown below. They represent physical volume approximations and are **not** energy-equivalence factors.[12]

| Standardized product | Factor, barrels per metric tonne | Source product label in the UN table |
|---|---:|---|
| Gasoline | 8.50 | Motor gasolene |
| Jet fuel | 7.77 | Jet fuel |
| Diesel | 7.23 | Gas diesel oil |

For a country’s product mapping, use the gasoline factor only for the gasoline field, the jet-fuel factor only for the jet-fuel field, and the gas-diesel-oil factor only for the diesel field. South Korea is already reported in barrels, so do not apply a density factor. The United States series is already a daily rate, so do not divide by month length.

## Transformation pseudocode

```python
# Inputs: clean levels with one row per country/month and source level columns.
for country in countries:
    assert complete_monthly_sequence(country, start='2020-07-01')
    assert no_duplicate_dates(country)
    assert no_missing([gasoline_level, jet_fuel_level, diesel_level])

    for product in ['gasoline', 'jet_fuel', 'diesel']:
        country[f'{product}_mbd'] = convert_to_calendar_day_mbd(
            levels=country[f'{product}_level'],
            dates=country['date'],
            native_unit=country['native_unit'],
            product=product,
        )
        country[f'{product}_yoy_mbd'] = (
            country[f'{product}_mbd'] - country[f'{product}_mbd'].shift(12)
        )

    country['net_yoy_mbd'] = country[
        ['gasoline_yoy_mbd', 'jet_fuel_yoy_mbd', 'diesel_yoy_mbd']
    ].sum(axis=1)
    chart_data = country[country['date'] >= '2021-07-01']
```

For leap-year February or any cross-year pair with different month lengths, calculate each monthly rate with that month’s own days before subtracting. This avoids subtly overstating or understating year-over-year daily-rate changes.

## Required validation

Run the following tests before plotting.

1. Assert a continuous first-of-month monthly sequence from July 2020 through the latest published month for each country.
2. Assert exactly one row per country/date and three non-null numeric product levels.
3. Assert a single declared native unit per country per refresh.
4. Assert all displayed months have three non-null `*_yoy_mbd` values.
5. Assert `abs(net_yoy_mbd − gasoline_yoy_mbd − jet_fuel_yoy_mbd − diesel_yoy_mbd) ≤ 1e-8` after CSV serialization.
6. Cross-check the final available month’s source levels against the primary source report or table and record the check.
7. Preserve raw downloads, extraction code, source URL, retrieval time, release date, and checksum where practical.
8. If a source’s latest month is delayed, chart through the prior verified month and place a delay note in the catalog; do not use a weekly estimate or a third-party replacement.

## Chart footer text

Place a visually separated **DATA SOURCE & METHODOLOGY** panel below every country chart, using country-specific substitutions. Use a light-gray background and four readable lines; the panel must not overlap the x-axis labels or the plot.

> Source: `{official publisher}`. Dataset: `{official measure}`. Vintage: `{release or modification date}`.
>
> Official dataset: `{official source URL}` (full URL in accompanying source catalog). Native series: `{exact mapping}` [`{native unit}`].
>
> Calculation: `{country-specific formula}` Monthly levels are converted to calendar-day-average M b/d; YoY = current month minus same calendar month one year earlier.
>
> Interpretation: colored bars are product-level YoY contributions; black line is their three-product sum, not total national demand. Scope note: `{country-specific caveat}`.

Place this statement in the shared small-multiples chart footer:

> Official national data; bars show product-level YoY change in calendar-day-average M b/d and the black line is the three-product sum. Measures differ by country; see the methodology guide for sources, mappings, conversions, and caveats.

## Material comparability caveats

Use country-specific source names rather than calling every series “consumption.” U.S. and Canada **products supplied** are supply-chain demand proxies. Brazil, Japan, Mexico, and Australia are **sales** series. The United Kingdom reports **inland deliveries**. India, Spain, and South Korea report **consumption/market-demand** measures. The standardized physical-unit conversion improves comparability, but it does not make these source concepts identical.

The diesel field is a practical transport-fuel proxy. Canada’s distillate fuel oil can include non-road components. The United Kingdom combines white and red diesel. Australia’s diesel-oil total is potentially broader than automotive diesel. Japan’s gas oil is its official diesel proxy. Motor Spirit/Gasolina C can include biofuel blending.

## Monthly recurring prompt

Use this prompt for a monthly refresh task:

```text
Refresh the International Transport-Fuel Market Indicators package. Follow the installed international-fuel-demand-charts skill and its country-source playbook. Pull the current official full historical series for the United States, India, Brazil, Japan, Mexico, United Kingdom, Spain, Australia, South Korea, and Canada. Use July 2020 through each country’s latest official month as the input history. Create standard country charts beginning July 2021 and ending at each country’s own latest verified month. Use calendar-day-average million b/d, the documented product mappings and conversion factors, signed orange/blue/green stacks, black net line, adaptive y-axis per country, a readable four-line in-chart source/methodology/scope footer, latest-month annotation, and a small-multiples chart. Validate source continuity, conversion arithmetic, final-month levels, and PNG layout. Deliver one ZIP with PNG, PDF, standardized master CSV, catalog, raw-data provenance, and a concise summary stating each country’s data cutoff, latest net YoY M b/d, delays, and revisions. Never fill a missing official month with an estimate.
```

## References

[1]: https://www.eia.gov/dnav/pet/xls/PET_CONS_PSUP_DC_NUS_MBBLPD_M.xls "EIA U.S. Product Supplied monthly history workbook"
[2]: https://www.eia.gov/dnav/pet/pet_cons_psup_dc_nus_mbblpd_m.htm "EIA U.S. Product Supplied for Crude Oil and Petroleum Products"
[3]: https://ppac.gov.in/consumption/products-wise "PPAC Domestic Consumption of Petroleum Products"
[4]: https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-combustiveis-m3-1990-2025.csv "ANP fuel sales data"
[5]: https://www.meti.go.jp/english/statistics/tyo/sekiyuso/index.html "METI Petroleum Statistics"
[6]: https://ebdi.pemex.com/bdi/bdiController.do?action=cuadro&cvecua=RVOLVIN "PEMEX Volumen de ventas internas"
[7]: https://assets.publishing.service.gov.uk/media/6a8ecd5c01bbff0bf8f97a07/ET_3.13_AUG_26.xlsx "DESNZ Energy Trends Table 3.13"
[8]: https://www.cores.es/sites/default/files/archivos/estadisticas/est-petroliferos-consumo-2026.xls "CORES petroleum-product consumption workbook"
[9]: https://www.energy.gov.au/energy-data/australian-petroleum-statistics "Australian Petroleum Statistics"
[10]: https://www.petronet.co.kr/v4/excel/KDCQ0200_x.jsp?term=m&by=2020&bq=3&bm=07&ay=2026&aq=2&am=06&ProdCDList=B000,C000,D000,E000,F000,G000,H000,J000,L000,N000,I000,M000,K000,O000,S000 "KNOC Petronet Domestic Consumption by Product extract"
[11]: https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2510008101 "Statistics Canada Table 25-10-0081-01"
[12]: https://unstats.un.org/unsd/energy/yearbook/conversion.htm "United Nations Statistics Division petroleum-product conversion factors"
