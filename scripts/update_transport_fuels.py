"""Validate source extracts and publish one consistent chart/CSV/status bundle.

The existing scheduled GitHub Actions refresh retrieves the official sources and
supplies a manifest. This script deliberately does not scrape or guess changing
source layouts.
"""
import argparse
import calendar
import copy
import csv
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
PRODUCTS = ('gasoline', 'jet_fuel', 'diesel')
FACTORS = dict(zip(PRODUCTS, (8.50, 7.77, 7.23)))
START = '2020-07-01'
CHART_START = '2021-07-01'
OFFICIAL_DOMAINS = {
    'United States': ('eia.gov',), 'India': ('ppac.gov.in',),
    'Brazil': ('gov.br',), 'Japan': ('meti.go.jp', 'e-stat.go.jp'),
    'Mexico': ('pemex.com',), 'United Kingdom': ('gov.uk', 'publishing.service.gov.uk'),
    'Spain': ('cores.es',), 'Australia': ('energy.gov.au',),
    'South Korea': ('petronet.co.kr', 'knoc.co.kr'), 'Canada': ('statcan.gc.ca',),
}


def next_month(value):
    d = date.fromisoformat(value)
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1).isoformat()


def convert(value, unit, product, period):
    days = calendar.monthrange(int(period[:4]), int(period[5:7]))[1]
    if unit == 'thousand b/d':
        return value / 1000
    multipliers = {'thousand barrels': 1000, 'm3': 6.28981077, 'kl': 6.28981077,
                   'ML': 6289.81077, 'tonnes': FACTORS[product],
                   'kt': FACTORS[product] * 1000, 'TMT': FACTORS[product] * 1000}
    return value * multipliers[unit] / days / 1_000_000


def validated_rows(path, unit, today):
    with path.open(newline='', encoding='utf-8-sig') as handle:
        native = list(csv.DictReader(handle))
    if not native:
        raise ValueError('Empty source extract')
    rows = []
    expected = START
    for row in native:
        period = row['date']
        if period != expected:
            raise ValueError(f'Expected {expected}; got {period} (gap, duplicate, or out of order)')
        if date.fromisoformat(period).replace(day=1) >= today.replace(day=1):
            raise ValueError('Unpublished/current or future month in monthly source')
        if row['native_unit'] != unit:
            raise ValueError('Native unit differs from the established country mapping')
        result = {'date': period}
        for product in PRODUCTS:
            value = float(row[product])
            if not math.isfinite(value) or value < 0:
                raise ValueError(f'Invalid {product} value for {period}')
            result[product] = value
            result[product + '_mbd'] = convert(value, unit, product, period)
            if not math.isfinite(result[product + '_mbd']):
                raise ValueError(f'Non-finite converted {product} value for {period}')
        rows.append(result)
        expected = next_month(period)
    by_date = {r['date']: r for r in rows}
    output = []
    for row in rows:
        if row['date'] < CHART_START:
            continue
        prior = by_date[str(int(row['date'][:4]) - 1) + row['date'][4:]]
        for product in PRODUCTS:
            row[product + '_yoy_mbd'] = row[product + '_mbd'] - prior[product + '_mbd']
            if not math.isfinite(row[product + '_yoy_mbd']):
                raise ValueError(f'Non-finite {product} YoY value for {row["date"]}')
        row['net_yoy_mbd'] = sum(row[p + '_yoy_mbd'] for p in PRODUCTS)
        if not math.isfinite(row['net_yoy_mbd']):
            raise ValueError(f'Non-finite net YoY value for {row["date"]}')
        output.append(row)
    if not output:
        raise ValueError('Need at least 13 months of history')
    return output


def write_json(path, data):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + '\n')
    temp.replace(path)


def write_csv(path, rows, fields):
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    temp.replace(path)


def apply_manifest(manifest_path, root=ROOT, now=None):
    now = now or datetime.now(timezone.utc)
    timestamp = now.isoformat()
    directory = root / 'static/transport-fuels'
    manifest = json.loads(manifest_path.read_text())
    data = json.loads((directory / 'data.json').read_text())
    with (directory / 'standardized_transport_fuel_yoy_mbd_data.csv').open(newline='') as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        master = list(reader)
    countries = {c['country'] for c in data}
    if set(manifest) != countries:
        raise ValueError('Manifest must account for all ten countries, including failures')
    status_path = directory / 'refresh-status.json'
    status = json.loads(status_path.read_text()) if status_path.exists() else {'countries': {}}
    status['last_run_at'] = timestamp
    failures = []
    for c in data:
        name = c['country']
        item = manifest[name]
        state = copy.deepcopy(status['countries'].get(name, {}))
        state.update(last_attempt_at=timestamp, latest_month=c['rows'][-1]['date'])
        try:
            if item.get('status') != 'ok':
                raise ValueError(item.get('message') or 'Source check did not succeed')
            urls = item['source_urls']
            if not urls or any(
                urlsplit(url).scheme != 'https' or urlsplit(url).username or
                not any((urlsplit(url).hostname or '') == domain or
                        (urlsplit(url).hostname or '').endswith('.' + domain)
                        for domain in OFFICIAL_DOMAINS[name]) for url in urls):
                raise ValueError('Require exact official HTTPS download URLs')
            if not item.get('vintage') or not item.get('validation_note'):
                raise ValueError('Require release vintage and final-month validation note')
            source_path = (manifest_path.parent / item['csv']).resolve()
            source_bytes = source_path.read_bytes()
            unit = next(r['native_unit'] for r in master if r['country'] == name)
            rows = validated_rows(source_path, unit, now.date())
            if rows[-1]['date'] < c['rows'][-1]['date']:
                raise ValueError('Source extract would truncate existing history')
            # A gross unit mismatch must not silently replace the chart history.
            prior_rows = {r['date']: r for r in c['rows']}
            for r in rows:
                old = prior_rows.get(r['date'])
                if old:
                    for product in PRODUCTS:
                        old_value = old[product + '_mbd']
                        if old_value > 0 and not 0.1 <= r[product + '_mbd'] / old_value <= 10:
                            raise ValueError(f'Gross level change needs review: {r["date"]} {product}')
            native_dir = directory / 'source-levels'
            native_dir.mkdir(exist_ok=True)
            chart_rows = [{k: v for k, v in r.items() if k == 'date' or k.endswith('_mbd')} for r in rows]
            changed = chart_rows != c['rows']
            metadata = {k: v for k, v in c.items() if k != 'rows'}
            metadata['vintage'] = item['vintage']
            metadata['source_url'] = urls[0]
            # Preserve product definitions and point source links at the verified download.
            new_master = [{**metadata, 'native_unit': unit, **r} for r in rows]
            # Prepare all values before mutating in-memory country/master rows.
            # The source bytes are read once, so a later filesystem failure
            # cannot leave a country half-updated in the published bundle.
            source_target = native_dir / (name.lower().replace(' ', '_') + '.csv')
            source_temp = source_target.with_suffix('.csv.tmp')
            source_temp.write_bytes(source_bytes)
            source_temp.replace(source_target)
            master = [r for r in master if r['country'] != name] + new_master
            c.update(vintage=item['vintage'], source_url=urls[0], rows=chart_rows)
            state.update(status='ok', message='Validated official source; new months and revisions checked.',
                         last_success_at=timestamp, latest_month=rows[-1]['date'],
                         source_urls=urls, vintage=item['vintage'], validation_note=item['validation_note'],
                         source_extract_sha256=hashlib.sha256(source_bytes).hexdigest())
            if changed:
                state['last_data_change_at'] = timestamp
        except (ValueError, KeyError, OSError, TypeError, csv.Error) as exc:
            state.update(status='error', message=str(exc)[:500])
            failures.append(name)
        status['countries'][name] = state
    # One Git commit/deployment publishes these files together. A failed country's
    # original records and last-success timestamp remain untouched.
    write_json(directory / 'data.json', data)
    master.sort(key=lambda r: (r['country'], r['date']))
    write_csv(directory / 'standardized_transport_fuel_yoy_mbd_data.csv', master, fields)
    with (directory / 'standardized_chart_catalog.csv').open(newline='') as handle:
        reader = csv.DictReader(handle)
        catalog_fields = reader.fieldnames
        catalog = list(reader)
    for row in catalog:
        country = next(c for c in data if c['country'] == row['country'])
        last = country['rows'][-1]
        row.update(latest_month=last['date'][:7], standardized_period='2021-07 to ' + last['date'][:7], vintage=country['vintage'], source_url=country['source_url'])
        for key in ('gasoline_yoy_mbd', 'jet_fuel_yoy_mbd', 'diesel_yoy_mbd', 'net_yoy_mbd'):
            row[key] = last[key]
    write_csv(directory / 'standardized_chart_catalog.csv', catalog, catalog_fields)
    write_json(status_path, status)
    return failures


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    args = parser.parse_args()
    failures = apply_manifest(args.manifest.resolve())
    print(json.dumps({'failed_countries': failures, 'published_countries': 10 - len(failures)}))
    # Nonzero reports partial failure. Valid countries and failure statuses must
    # still be committed by the scheduled refresh; do not discard the bundle.
    raise SystemExit(1 if failures else 0)
