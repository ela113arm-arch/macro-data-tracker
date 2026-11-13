"""Refresh public daily transport indicators and China's weekly logistics data.

Run independently of the slower macro ETL. A failed source retains its last
validated observations and records the failure; the CLI exits nonzero afterward.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
from html import unescape
import json
import logging
import math
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'static' / 'transport-demand'
TSA_URL = 'https://www.tsa.gov/travel/passenger-volumes'
TSA_COPY = 'https://passengerindex.com/web/tsa/'
EURO_URL = 'https://www.eurocontrol.int/Economics/tfc_ct_data.json'
CHINA_URL = 'https://www.mot.gov.cn/zhuanti/wuliubtbc/qingkuangtongbao_wuliu/'
LOG = logging.getLogger('transport-demand')


def text_only(value):
    return unescape(re.sub(r'<[^>]*>', ' ', value)).strip()


def download(url, raw_dir=None):
    """Retry temporary failures, but do not retry access-denied responses."""
    for attempt in range(3):
        try:
            req = Request(url, headers={'User-Agent': 'WillowdeskTransportData/1.0'})
            with urlopen(req, timeout=25) as response:
                body = response.read(15_000_001)
                if len(body) > 15_000_000:
                    raise ValueError('Source exceeds the 15 MB response limit')
            if raw_dir:
                raw_dir.mkdir(parents=True, exist_ok=True)
                name = hashlib.sha256(url.encode()).hexdigest()[:16] + '.raw'
                (raw_dir / name).write_bytes(body)
            return body
        except (HTTPError, URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 2:
                raise
            time.sleep(attempt + 1)


def parse_tsa(html):
    """Read dated rows, excluding monthly summaries and prior-year columns."""
    rows = {}
    for tr in re.findall(r'<tr\b[^>]*>(.*?)</tr>', html, re.I | re.S):
        cells = [text_only(x) for x in re.findall(r'<t[dh]\b[^>]*>(.*?)</t[dh]>', tr, re.I | re.S)]
        if len(cells) < 2:
            continue
        observed = None
        for fmt in ('%m/%d/%Y', '%B %d, %Y', '%Y-%m-%d'):
            try:
                observed = datetime.strptime(cells[0], fmt).date().isoformat()
                break
            except ValueError:
                pass
        if observed and re.fullmatch(r'[\d,]+', cells[1]):
            rows[observed] = {'date': observed, 'value': int(cells[1].replace(',', ''))}
    if len(rows) < 14:
        raise ValueError('TSA table has fewer than 14 valid daily observations')
    return sorted(rows.values(), key=lambda r: r['date'])


def publication_date(html):
    match = re.search(r'<meta[^>]+name=["\']PubDate["\'][^>]+content=["\']([^"\']+)', html, re.I)
    if match:
        return match.group(1)[:10]
    for block in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, re.S):
        match = re.search(r'"dateModified"\s*:\s*"(\d{4}-\d{2}-\d{2})', block)
        if match:
            return match.group(1)
    return None


def fetch_tsa(raw_dir=None):
    errors = []
    for url in (TSA_URL, TSA_COPY):
        try:
            body = download(url, raw_dir)
            html = body.decode('utf-8-sig')
            result = {'series': {'tsa_passengers': parse_tsa(html)}, 'source_url': url,
                      'source_published_date': publication_date(html),
                      'snapshot_sha256': hashlib.sha256(body).hexdigest()}
            if url == TSA_COPY:
                result.update(source_name='TSA via Passenger Index',
                              attribution='TSA data compiled by Passenger Index, CC BY 4.0.',
                              notice='Using the public Passenger Index copy because the direct TSA table could not be retrieved.')
            else:
                result.update(source_name='Transportation Security Administration', attribution='TSA; U.S. public-domain data.', notice='')
            return result
        except Exception as exc:
            errors.append(f'{urlsplit(url).hostname}: {exc}')
    raise RuntimeError('; '.join(errors))


def parse_eurocontrol(payload):
    if not isinstance(payload, list) or len(payload) < 3:
        raise ValueError('EUROCONTROL did not return its tabular JSON dataset')
    columns = payload[0]
    required = ('Entity', 'Day', 'Flights')
    if not all(c in columns for c in required):
        raise ValueError('EUROCONTROL columns changed')
    ix = {c: columns.index(c) for c in required}
    ref = next((i for i, c in enumerate(columns) if re.fullmatch(r'Flights \d{4} \(Reference\)', c) and '2019' not in c), None)
    ref_date = columns.index('Day Previous Year') if 'Day Previous Year' in columns else None
    rows = []
    for item in payload[2:]:
        if item[ix['Entity']] != '-Total Network Manager Area':
            continue
        r = {'date': item[ix['Day']].strip(), 'value': float(item[ix['Flights']])}
        if ref is not None and ref_date is not None and str(item[ref]).strip():
            r.update(reference_date=item[ref_date].strip(), reference_value=float(item[ref]))
        rows.append(r)
    if len(rows) < 14:
        raise ValueError('EUROCONTROL network series is missing or too short')
    return rows


def fetch_eurocontrol(raw_dir=None):
    body = download(EURO_URL, raw_dir)
    return {'series': {'europe_flights': parse_eurocontrol(json.loads(body))},
            'source_name': 'EUROCONTROL Network Manager', 'source_url': EURO_URL,
            'source_published_date': None, 'snapshot_sha256': hashlib.sha256(body).hexdigest(),
            'attribution': 'EUROCONTROL daily traffic variation; network total.',
            'notice': ''}


def china_links(html, base=CHINA_URL):
    links = []
    for href, title in re.findall(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, re.I | re.S):
        if '全国物流保通保畅运行情况' not in text_only(title):
            continue
        url = urljoin(base, href)
        if urlsplit(url).hostname != 'www.mot.gov.cn':
            continue
        if url not in links:
            links.append(url)
    return links


def parse_china(html, source_url):
    pub = publication_date(html)
    if not pub:
        raise ValueError('China report publication date missing')
    title = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.I | re.S)
    title = text_only(title.group(1)) if title else ''
    period = re.search(r'(\d{1,2})月(\d{1,2})日[—–－-](?:(\d{1,2})月)?(\d{1,2})日', title)
    if not period:
        raise ValueError('China report is not a dated weekly report')
    sm, sd, em, ed = period.groups()
    year = int(pub[:4])
    end = date(year, int(em or sm), int(ed))
    if end > date.fromisoformat(pub):
        end = end.replace(year=year - 1)
    start = date(end.year - (int(sm) > end.month), int(sm), int(sd))
    if (end - start).days != 6:
        raise ValueError('China report does not cover exactly seven days')
    content = text_only(html)
    patterns = {'china_trucks': r'全国高速公路货车通行\s*([\d,.]+)\s*(万)?辆',
                'china_flights': r'民航保障航班\s*([\d,.]+)\s*(万)?班'}
    out = {}
    for key, pattern in patterns.items():
        found = re.search(pattern, content)
        if not found:
            raise ValueError(f'China report missing {key}')
        value = float(found.group(1).replace(',', '')) * (10000 if found.group(2) else 1)
        out[key] = [{'date': end.isoformat(), 'period_start': start.isoformat(),
                     'value': round(value), 'published_date': pub, 'source_url': source_url}]
    return out


def fetch_china(raw_dir=None, backfill=False):
    pages = [CHINA_URL]
    # Two listing pages seed about seven months of weekly observations.
    if backfill:
        pages.append(urljoin(CHINA_URL, 'index_1.html'))
    urls = []
    for page in pages:
        urls.extend(china_links(download(page, raw_dir).decode('utf-8'), page))
    urls = list(dict.fromkeys(urls))
    if not urls:
        raise ValueError('China weekly report list is empty')
    # Refresh the latest three releases for revisions. Backfill only on first run.
    urls = urls if backfill else urls[:3]
    series = {'china_trucks': [], 'china_flights': []}
    snapshots = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        bodies = list(pool.map(lambda u: download(u, raw_dir), urls))
    for url, body in zip(urls, bodies):
        parsed = parse_china(body.decode('utf-8'), url)
        for key, values in parsed.items():
            series[key].extend(values)
        snapshots.append(hashlib.sha256(body).hexdigest())
    return {'series': series, 'source_name': 'China Ministry of Transport',
            'source_url': CHINA_URL, 'source_published_date': max(r['published_date'] for r in series['china_trucks']),
            'snapshot_sha256': hashlib.sha256(''.join(snapshots).encode()).hexdigest(),
            'attribution': 'Ministry of Transport of the People’s Republic of China.',
            'notice': 'China is the weekly exception: each point is a seven-day total, dated at the week ending.'}


SERIES = {
    'tsa_passengers': {'title': 'U.S. airport passengers', 'unit': 'passengers/day', 'frequency': 'daily', 'fuel': 'Jet fuel',
                       'description': 'TSA checkpoint screenings. Passenger activity; excludes cargo and does not measure fuel uplift.'},
    'europe_flights': {'title': 'European flight activity', 'unit': 'flights/day', 'frequency': 'daily', 'fuel': 'Jet fuel',
                       'description': 'EUROCONTROL network total, including overflights. Prior-year comparison uses the source’s matched weekday; countries must not be summed.'},
    'china_trucks': {'title': 'China highway truck traffic', 'unit': 'truck passages/week', 'frequency': 'weekly', 'fuel': 'Diesel',
                     'description': 'Highway truck passages, not unique trucks or kilometres. Electric trucks and changing route lengths affect the link to diesel.'},
    'china_flights': {'title': 'China flight activity', 'unit': 'flights/week', 'frequency': 'weekly', 'fuel': 'Jet fuel',
                      'description': 'Civil aviation flights reported by the transport ministry. Weekly totals include cargo flights.'},
}
FEED_KEYS = {'tsa': ('tsa_passengers',), 'eurocontrol': ('europe_flights',), 'china': ('china_trucks', 'china_flights')}


def validated_rows(rows, today, key):
    result = {}
    maximum = 120_000_000 if key == 'china_trucks' else 200_000 if key != 'tsa_passengers' else 6_000_000
    for original in rows:
        row = dict(original)
        observed = date.fromisoformat(row['date'])
        value = float(row['value'])
        if observed >= today or not math.isfinite(value) or not 0 < value <= maximum:
            raise ValueError(f'Invalid date or value in {key}: {row}')
        if 'reference_value' in row and (not math.isfinite(float(row['reference_value'])) or float(row['reference_value']) < 0):
            raise ValueError('Invalid previous-year reference value')
        if 'reference_date' in row and date.fromisoformat(row['reference_date']) >= observed:
            raise ValueError('Previous-year reference date must precede observation')
        if 'published_date' in row and not observed <= date.fromisoformat(row['published_date']) <= today:
            raise ValueError('Invalid report publication date')
        if 'period_start' in row and (observed - date.fromisoformat(row['period_start'])).days != 6:
            raise ValueError('Weekly totals must cover exactly seven days')
        if row['date'] in result and result[row['date']] != row:
            raise ValueError(f'Conflicting duplicate date in {key}')
        result[row['date']] = row
    if not result:
        raise ValueError(f'No valid rows for {key}')
    return [result[d] for d in sorted(result)]


def merge_rows(existing, incoming):
    rows = {r['date']: r for r in existing}
    rows.update({r['date']: r for r in incoming})
    return [rows[d] for d in sorted(rows)]


def write_json(path, payload):
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(',', ':')) + '\n', encoding='utf-8')
    temporary.replace(path)


def refresh(output=OUTPUT, now=None, fetchers=None, raw_dir=None):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    now = now or datetime.now(timezone.utc)
    timestamp = now.isoformat()
    path = output / 'data.json'
    previous = json.loads(path.read_text()) if path.exists() else {'version': 1, 'feeds': {}, 'series': {}}
    payload = copy.deepcopy(previous)
    payload['last_attempt_at'] = timestamp
    if fetchers is None:
        fetchers = {'tsa': lambda: fetch_tsa(raw_dir), 'eurocontrol': lambda: fetch_eurocontrol(raw_dir),
                    'china': lambda: fetch_china(raw_dir, backfill=not previous['series'].get('china_trucks', {}).get('rows'))}
    failures = []
    for feed, fetcher in fetchers.items():
        prior_feed = previous['feeds'].get(feed, {})
        state = dict(prior_feed, last_attempt_at=timestamp)
        try:
            fetched = fetcher()
            updates = {}
            for key in FEED_KEYS[feed]:
                incoming = validated_rows(fetched['series'][key], now.date(), key)
                old = previous['series'].get(key, {}).get('rows', [])
                if old and incoming[-1]['date'] < old[-1]['date']:
                    raise ValueError(f'{key}: source would move the latest observation backwards')
                # Record the retrieval provenance of each new/revised observation.
                for r in incoming:
                    r.setdefault('source_url', fetched['source_url'])
                merged = merge_rows(old, incoming)
                updates[key] = dict(SERIES[key], feed=feed, rows=merged)
            latest = min(v['rows'][-1]['date'] for v in updates.values())
            limit = 14 if feed == 'china' else 4
            if (now.date() - date.fromisoformat(latest)).days > limit:
                raise ValueError(f'Source is stale: latest observation {latest}')
            state.update({k: v for k, v in fetched.items() if k != 'series'})
            state.update(status='ok', error=None, last_success_at=timestamp, latest_observation=latest)
            payload['series'].update(updates)
            LOG.info('%s: validated through %s', feed, latest)
        except Exception as exc:
            state.update(status='error', error=str(exc))
            failures.append(feed)
            LOG.error('%s: %s; retaining last validated history', feed, exc)
        payload['feeds'][feed] = state
    write_json(path, payload)
    fields = ['date', 'value', 'reference_date', 'reference_value', 'period_start', 'published_date', 'source_url']
    for key, series in payload['series'].items():
        target = output / (key + '.csv')
        temporary = target.with_suffix('.csv.tmp')
        with temporary.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(series['rows'])
        temporary.replace(target)
    return failures


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT)
    parser.add_argument('--raw-dir', type=Path)
    args = parser.parse_args()
    raise SystemExit(bool(refresh(args.output, raw_dir=args.raw_dir)))
