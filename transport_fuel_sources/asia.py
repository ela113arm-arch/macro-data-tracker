"""Official India, Japan, and South Korea transport-fuel adapters.

The three publishers use HTML/JSON and spreadsheet exports with layouts that
change independently.  These parsers deliberately validate the product and
unit labels before selecting a value, so an upstream layout change fails the
country refresh instead of silently changing the series.
"""

from datetime import date, datetime, timezone
import io
import re
import unicodedata
from urllib.parse import urlencode, urljoin

import pandas as pd
from bs4 import BeautifulSoup

from . import PRODUCTS, START, iso_month, parse_number, validate_rows


PPAC_PAGE = "https://ppac.gov.in/consumption/products-wise"
PPAC_DATA = "https://ppac.gov.in/AjaxController/getConsumptionPetroleumProductsData"
KNOC_URL = "https://www.petronet.co.kr/v4/excel/KDCQ0200_x.jsp"
METI_PAGE = "https://www.meti.go.jp/english/statistics/tyo/sekiyuso/index.html"
METI_ESTAT = "https://www.e-stat.go.jp/stat-search/file-download?statInfId=000040491293&fileKind=0"
KNOC_CODES = ("B000", "C000", "D000", "E000", "F000", "G000", "H000", "J000", "L000", "N000", "I000", "M000", "K000", "O000", "S000")


def _today(value=None):
    if value is None:
        return datetime.now(timezone.utc).date()
    return value.date() if isinstance(value, datetime) else value


def _last_completed(value=None):
    today = _today(value)
    if today.month == 1:
        return date(today.year - 1, 12, 1)
    return date(today.year, today.month - 1, 1)


def _get(session, url):
    response = session.get(url, timeout=(10, 45))
    response.raise_for_status()
    if not response.content:
        raise ValueError(f"Empty response from official source: {url}")
    return response


def _post(session, url, data):
    response = session.post(url, data=data, timeout=(10, 45), headers={"X-Requested-With": "XMLHttpRequest"})
    response.raise_for_status()
    if not response.content:
        raise ValueError(f"Empty response from official source: {url}")
    return response


def _fy(start_year):
    return f"{start_year}-{start_year + 1}"


def _india_rows(session, today=None):
    landing = _get(session, PPAC_PAGE)
    soup = BeautifulSoup(landing.content, "html.parser")
    page_id = soup.find("input", id="page_id")
    page_id = page_id.get("value") if page_id else "43"
    if page_id != "43":
        raise ValueError(f"PPAC products-wise page id changed: {page_id!r}")
    report = soup.find("select", id="getReportBy")
    if report is not None and not any((o.get("value") or "") == "1" for o in report.find_all("option")):
        raise ValueError("PPAC products-wise page does not advertise reportBy=1")

    cutoff = _last_completed(today)
    current_fy = cutoff.year if cutoff.month >= 4 else cutoff.year - 1
    rows = {}
    vintage = []
    for year in range(2020, current_fy + 1):
        response = _post(session, PPAC_DATA, {"financialYear": _fy(year), "reportBy": "1", "pageId": page_id})
        try:
            result = response.json().get("result")
        except Exception as exc:
            raise ValueError(f"PPAC {_fy(year)} response is not JSON: {exc}") from exc
        if isinstance(result, list):
            records = result
        elif isinstance(result, dict):
            records = list(result.values())
        else:
            raise ValueError(f"PPAC {_fy(year)} returned no result records")
        by_title = {}
        for record in records:
            title = str(record.get("title", "")).strip().upper()
            if title in {"MS", "ATF", "HSD"}:
                by_title[title] = record
        if set(by_title) != {"MS", "ATF", "HSD"}:
            raise ValueError(f"PPAC {_fy(year)} missing MS/ATF/HSD rows")
        modified = {str(r.get("modified_date", "")).strip() for r in by_title.values() if r.get("modified_date")}
        if modified:
            vintage.extend(sorted(modified))
        for month, name in enumerate(("april", "may", "june", "july", "august", "september", "october", "november", "december", "january", "february", "march"), 4):
            target_year = year if month <= 12 else year + 1
            if month > 12:
                month -= 12
            period = date(target_year, month, 1)
            values = {"gasoline": by_title["MS"].get(name), "jet_fuel": by_title["ATF"].get(name), "diesel": by_title["HSD"].get(name)}
            parsed = {key: parse_number(value) for key, value in values.items()}
            if period > cutoff:
                if any(value is not None for value in parsed.values()):
                    raise ValueError(f"PPAC contains a partial unpublished month {period.isoformat()}")
                continue
            if all(value is None for value in parsed.values()):
                continue
            if any(value is None for value in parsed.values()):
                raise ValueError(f"PPAC row {period.isoformat()} has an incomplete MS/ATF/HSD set")
            rows[period.isoformat()] = {"date": period.isoformat(), **parsed}
    if not rows:
        raise ValueError("PPAC returned no published monthly rows")
    # The first fiscal-year file begins in April 2020. Drop those pre-July
    # months before applying the package's July 2020 continuous-month check.
    rows = {period: row for period, row in rows.items() if period >= START}
    clean = validate_rows(rows.values(), "TMT")
    return clean, sorted(set(vintage))


def fetch_india(session, today=None):
    rows, modified = _india_rows(session, today)
    latest = rows[-1]["date"][:7]
    release = ", ".join(modified) if modified else "release date not supplied"
    return {
        "rows": rows,
        "native_unit": "TMT",
        "source_urls": [PPAC_PAGE, PPAC_DATA],
        "vintage": f"PPAC domestic consumption retrieved {_today(today).isoformat()}; source modified {release}; data through {latest}.",
        "validation_note": f"PPAC reportBy=1 pageId=43; exact titles MS, ATF, HSD and April-March fiscal columns mapped to calendar months. Final native row {latest}: {rows[-1]['gasoline']}, {rows[-1]['jet_fuel']}, {rows[-1]['diesel']} TMT.",
    }


def _norm(value):
    text = "" if value is None or (isinstance(value, float) and pd.isna(value)) else str(value)
    text = unicodedata.normalize("NFKD", text).casefold()
    return " ".join(text.replace("\u00a0", " ").split())


def _japan_month(value):
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.date().replace(day=1).isoformat() if hasattr(value, "date") else value.replace(day=1).isoformat()
    text = str(value).strip().replace("年", "-").replace("月", "")
    m = re.search(r"(20\d{2})\s*[-/.]\s*(\d{1,2})", text)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-01"
    return None


def _japan_rows(content, today=None):
    """Extract domestic-sales rows from a METI historical/current workbook."""
    try:
        book = pd.ExcelFile(io.BytesIO(content))
    except Exception as exc:
        raise ValueError(f"Unable to read METI petroleum workbook: {exc}") from exc
    found = {}
    for sheet in book.sheet_names:
        raw = pd.read_excel(book, sheet_name=sheet, header=None)
        month_cols = {}
        for _, row in raw.iterrows():
            for col, value in enumerate(row):
                period = _japan_month(value)
                if period:
                    month_cols[col] = period
        if not month_cols:
            continue
        all_text = " ".join(_norm(v) for v in raw.to_numpy().ravel() if pd.notna(v))
        domestic = ("国内販売" in all_text) or ("domestic sales" in all_text) or ("domestic sale" in all_text)
        if not domestic:
            continue
        targets = {"ガソリン": "gasoline", "gasoline": "gasoline", "ジェット燃料": "jet_fuel", "jet fuel": "jet_fuel", "航空タービン燃料": "jet_fuel", "軽油": "diesel", "gas oil": "diesel", "gasoil": "diesel"}
        candidate = {}
        for index, row in raw.iterrows():
            label = " ".join(_norm(v) for v in row.iloc[: min(6, len(row))] if pd.notna(v))
            product = next((mapped for name, mapped in targets.items() if name in label), None)
            if not product or product in candidate:
                continue
            # Product rows can be below a section label; a domestic marker in
            # the nearby rows confirms this is the sales section.
            nearby = " ".join(_norm(v) for v in raw.iloc[max(0, index - 5): index + 1].to_numpy().ravel() if pd.notna(v))
            if "国内販売" not in nearby and "domestic sales" not in nearby and "domestic sale" not in nearby:
                continue
            values = {period: parse_number(row.iloc[col]) for col, period in month_cols.items() if col < len(row)}
            candidate[product] = values
        if set(candidate) != set(PRODUCTS):
            continue
        for period in set.intersection(*(set(v) for v in candidate.values())):
            values = {product: candidate[product].get(period) for product in PRODUCTS}
            if all(value is not None for value in values.values()):
                found[period] = {"date": period, **values}
    if not found:
        raise ValueError("METI workbook has no validated Domestic Sales gasoline/jet fuel/gas oil rows")
    cutoff = _last_completed(today)
    for period in list(found):
        if date.fromisoformat(period) > cutoff:
            del found[period]
    return validate_rows(found.values(), "kl")


def _meti_links(html):
    links = re.findall(r"href\s*=\s*[\"']([^\"']+\.(?:xlsx?|xlsm)(?:\?[^\"']*)?)[\"']", html, flags=re.I)
    links = [urljoin(METI_PAGE, x) for x in links]
    # Prefer the historical workbook and current reports; exclude unrelated
    # statistics linked from the same landing page.
    selected = [x for x in links if re.search(r"seki|oil|petroleum|jikei|history|kakuhou|sokuhou", x, re.I)]
    return list(dict.fromkeys(selected or links))


def fetch_japan(session, today=None):
    try:
        landing = _get(session, METI_PAGE)
        links = _meti_links(landing.text)
    except Exception:
        links = [METI_ESTAT]
    if not links:
        links = [METI_ESTAT]
    if not links:
        raise ValueError("METI petroleum statistics page has no workbook links")
    merged = {}
    used = []
    errors = []
    # Final/revised reports sort first so overlap retains the final vintage.
    links.sort(key=lambda u: (0 if re.search(r"kakuhou|final|確報|histor", u, re.I) else 1, u))
    for url in links:
        try:
            content = _get(session, url).content
            rows = _japan_rows(content, today)
        except Exception as exc:
            errors.append(str(exc))
            continue
        used.append(url)
        for row in rows:
            merged.setdefault(row["date"], row)
    if not merged:
        raise ValueError("METI workbooks yielded no validated rows: " + (errors[-1] if errors else "unknown format"))
    rows = validate_rows(merged.values(), "kl")
    latest = rows[-1]["date"][:7]
    return {
        "rows": rows,
        "native_unit": "kl",
        "source_urls": [METI_PAGE, *used],
        "vintage": f"METI Petroleum Statistics retrieved {_today(today).isoformat()}; data through {latest}.",
        "validation_note": f"Domestic Sales only; gasoline, jet fuel and gas oil labels validated in workbook. Final native row {latest}: {rows[-1]['gasoline']}, {rows[-1]['jet_fuel']}, {rows[-1]['diesel']} kl; preliminary overlap retained only where no final row exists.",
    }


def _knoc_month(value, prior=None):
    text = str(value).replace("\u00a0", " ").strip()
    match = re.search(r"(\d{2,4})\s*년\s*(\d{1,2})\s*월", text)
    if match:
        year = int(match.group(1))
        year += 2000 if year < 100 else 0
        return date(year, int(match.group(2)), 1).isoformat()
    match = re.search(r"(\d{1,2})\s*월", text)
    if match and prior:
        year = int(prior[:4]) + (1 if int(match.group(1)) == 1 else 0)
        return date(year, int(match.group(1)), 1).isoformat()
    return None


def _knoc_rows(content, today=None):
    if isinstance(content, bytes):
        # Current Petronet answers are UTF-8 even though older extracts used
        # EUC-KR; retain the latter as a compatibility fallback.
        text = content.decode("utf-8", errors="replace")
        if "제품" not in text and "천" not in text:
            text = content.decode("euc-kr", errors="replace")
    else:
        text = str(content)
    if not re.search(r"천\s*Bbl|thousand\s*barrel", text, flags=re.I):
        raise ValueError("KNOC extract does not declare thousand-barrel units")
    soup = BeautifulSoup(text, "html.parser")
    table = None
    headers = None
    expected = ["휘발유", "등유", "경유", "A중유", "B중유", "C중유", "납사", "용제", "항공유", "LPG", "아스팔트", "윤활유", "기타제품", "부생연료유", "바이오연료"]
    for candidate in soup.find_all("table"):
        cells = [x.get_text(" ", strip=True) for x in candidate.find_all("tr")[0].find_all(["th", "td"])] if candidate.find_all("tr") else []
        if cells[:2] == ["월", "제품명"] and all(label in cells for label in ("휘발유", "경유", "항공유")):
            table, headers = candidate, cells
            break
    if table is None:
        raise ValueError("KNOC KDCQ0200 extract is missing the expected Korean product header")
    actual = headers[2:-1]
    if actual != expected:
        raise ValueError(f"KNOC product headers changed: {actual!r}")
    # The requested code order is the publisher's documented column order;
    # validating all labels above confirms code-to-product positions live.
    rows = {}
    prior = None
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        period = _knoc_month(cells[0].get_text(" ", strip=True), prior)
        if not period:
            continue  # percentage-share row
        values = [parse_number(cell.get_text(" ", strip=True)) for cell in cells[1:]]
        if len(values) < len(expected) + 1:
            raise ValueError(f"KNOC row {period} has fewer product columns than its header")
        values = values[: len(expected)]
        selected = {"gasoline": values[0], "jet_fuel": values[8], "diesel": values[2]}
        if all(value is None for value in selected.values()):
            continue
        if any(value is None for value in selected.values()):
            raise ValueError(f"KNOC row {period} has an incomplete gasoline/jet/diesel set")
        rows[period] = {"date": period, **selected}
        prior = period
    cutoff = _last_completed(today)
    rows = {period: row for period, row in rows.items() if date.fromisoformat(period) <= cutoff}
    if not rows:
        raise ValueError("KNOC extract contains no completed monthly rows")
    return validate_rows(rows.values(), "thousand barrels")


def fetch_south_korea(session, today=None):
    cutoff = _last_completed(today)
    params = {"term": "m", "by": "2020", "bq": "3", "bm": "07", "ay": str(cutoff.year), "aq": str((cutoff.month - 1) // 3 + 1), "am": f"{cutoff.month:02d}", "ProdCDList": ",".join(KNOC_CODES)}
    url = KNOC_URL + "?" + urlencode(params)
    response = _get(session, url)
    rows = _knoc_rows(response.content, today)
    latest = rows[-1]["date"][:7]
    return {
        "rows": rows,
        "native_unit": "thousand barrels",
        "source_urls": [url],
        "vintage": f"KNOC Petronet KDCQ0200 retrieved {_today(today).isoformat()}; data through {latest}.",
        "validation_note": f"KDCQ0200 monthly national product extract; returned headers confirm gasoline 휘발유, aviation fuel 항공유 and diesel 경유 positions for requested product codes. Final native row {latest}: {rows[-1]['gasoline']}, {rows[-1]['jet_fuel']}, {rows[-1]['diesel']} thousand barrels.",
    }
