"""Official United Kingdom and Spain transport-fuel source adapters.

The two publishers use deliberately different spreadsheet layouts.  The
parsers below validate the labels before selecting values so a layout change
causes a country refresh to fail instead of silently publishing another
series.
"""

from datetime import date, datetime, timezone
import io
import re
import unicodedata
from urllib.parse import urljoin

import pandas as pd

from . import PRODUCTS, START, iso_month, parse_number, validate_rows


UK_LANDING_URL = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
SPAIN_LANDING_URL = "https://www.cores.es/en/estadisticas"


def _get(session, url):
    response = session.get(url, timeout=90)
    response.raise_for_status()
    if not response.content:
        raise ValueError(f"Empty response from official source: {url}")
    return response


def _as_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _uk_month(value):
    parsed = _as_date(value)
    if parsed:
        return parsed.replace(day=1).isoformat()
    text = re.sub(r"\s*\[[^]]+\]\s*$", "", str(value).strip())
    for fmt in ("%B %Y", "%b %Y"):
        try:
            return datetime.strptime(text, fmt).date().replace(day=1).isoformat()
        except ValueError:
            pass
    raise ValueError(f"Unrecognised UK monthly date: {value!r}")


def _month_limit(today):
    """First day of the current month, used to reject future source rows."""
    if today is None:
        today = datetime.now(timezone.utc).date()
    if isinstance(today, datetime):
        today = today.date()
    return today.replace(day=1).isoformat()


def _find_link(html, pattern, base):
    links = re.findall(r"href\s*=\s*[\"']([^\"']+)[\"']", html, flags=re.I)
    matches = [urljoin(base, link.strip()) for link in links if re.search(pattern, link, flags=re.I)]
    if not matches:
        raise ValueError(f"Official page has no download matching {pattern!r}")
    # A duplicate link is common (the accessible label and the visible link).
    return list(dict.fromkeys(matches))[0]


def _uk_download(session):
    landing = _get(session, UK_LANDING_URL)
    return _find_link(landing.text, r"(?:^|/)ET_3\.13[^/]*\.xlsx?$", UK_LANDING_URL)


def _uk_rows(content, today=None):
    try:
        raw = pd.read_excel(io.BytesIO(content), sheet_name="Month", header=None)
    except Exception as exc:
        raise ValueError(f"Unable to read DESNZ ET 3.13 workbook: {exc}") from exc
    header_row = None
    for index, row in raw.iterrows():
        labels = {str(value).replace("\n", " ").strip().lower() for value in row if pd.notna(value)}
        if "date" in labels and "petrol" in labels and "jet fuel" in labels:
            header_row = index
            break
    if header_row is None:
        raise ValueError("DESNZ Month sheet is missing its Date/Petrol/Jet fuel header")
    headers = [str(value).replace("\n", " ").strip().lower() if pd.notna(value) else "" for value in raw.iloc[header_row]]

    def column(label, contains=False):
        found = [i for i, value in enumerate(headers) if (label in value if contains else value == label)]
        if len(found) != 1:
            raise ValueError(f"DESNZ Month sheet requires one {label!r} column; found {len(found)}")
        return found[0]

    date_col = column("date")
    petrol_col = column("petrol")
    jet_col = column("jet fuel")
    white_col = column("white diesel")
    red_col = column("red diesel (gas oil)", contains=True)
    rows = []
    limit = _month_limit(today)
    for _, record in raw.iloc[header_row + 1:].iterrows():
        if pd.isna(record.iloc[date_col]) or not str(record.iloc[date_col]).strip():
            continue
        period = _uk_month(record.iloc[date_col])
        values = {
            "gasoline": parse_number(record.iloc[petrol_col]),
            "jet_fuel": parse_number(record.iloc[jet_col]),
            "white": parse_number(record.iloc[white_col]),
            "red": parse_number(record.iloc[red_col]),
        }
        if all(value is None for value in values.values()):
            continue  # Unpublished tail in the official monthly sheet.
        if period >= limit:
            raise ValueError(f"DESNZ workbook contains current/future month {period}")
        if any(value is None for value in values.values()):
            raise ValueError(f"DESNZ row {period} has an incomplete Petrol/Jet/diesel set")
        rows.append({"date": period, "gasoline": values["gasoline"], "jet_fuel": values["jet_fuel"],
                     "diesel": values["white"] + values["red"]})
    rows = [row for row in rows if row["date"] >= START]
    return validate_rows(rows, "kt")


def _workbook_vintage(content, publisher):
    """Extract a publisher-supplied release/update note where present."""
    try:
        raw = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None)
        text = " ".join(str(value) for value in raw.to_numpy().ravel() if pd.notna(value))
    except Exception:
        text = ""
    match = re.search(r"(?:published|updated|Actualizado el)\s+[^\n]*?(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})", text, flags=re.I)
    release = match.group(1) if match else "release date not supplied"
    return f"{publisher} workbook {release}"


def fetch_uk(session, today=None):
    """Fetch DESNZ ET 3.13 monthly inland deliveries in thousand tonnes."""
    url = _uk_download(session)
    content = _get(session, url).content
    rows = _uk_rows(content, today=today)
    latest = rows[-1]["date"][:7]
    return {
        "rows": rows,
        "source_urls": [url],
        "vintage": f"{_workbook_vintage(content, 'DESNZ ET 3.13')} retrieved {_month_limit(today)}; data through {latest}.",
        "validation_note": f"DESNZ Month sheet labels Petrol, Jet fuel, White diesel and Red diesel (Gas oil); diesel is white plus red. Final native row {latest}: {rows[-1]['gasoline']}, {rows[-1]['jet_fuel']}, {rows[-1]['diesel']} kt.",
    }


def _normalise_label(value):
    text = "" if value is None or pd.isna(value) else str(value)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return " ".join(text.casefold().split())


def _spanish_number(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("\xa0", "")
    if not text or text in {"-", "--", "…", "..."}:
        return None
    # CORES uses 1.234,56; accept the opposite convention for test extracts
    # while retaining a strict numeric result.
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".") if text.rfind(",") > text.rfind(".") else text.replace(",", "")
    elif "," in text:
        tail = text.rsplit(",", 1)[1]
        text = text.replace(",", ".") if len(tail) != 3 else text.replace(",", "")
    return parse_number(text)


def _spain_month(value):
    parsed = _as_date(value)
    if parsed:
        return parsed.replace(day=1).isoformat()
    text = str(value).strip()
    match = re.search(r"(20\d{2})[-/.](\d{1,2})", text)
    if match:
        return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-01"
    raise ValueError(f"Unrecognised CORES month: {value!r}")


def _spain_rows(content, requested_year):
    try:
        raw = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None)
    except Exception as exc:
        raise ValueError(f"Unable to read CORES consumption workbook {requested_year}: {exc}") from exc
    text = " ".join(str(value) for value in raw.to_numpy().ravel() if pd.notna(value))
    if not re.search(r"unidad\s*:\s*toneladas", text, flags=re.I):
        if not re.search(r"unit\s*:\s*tonnes", text, flags=re.I):
            raise ValueError(f"CORES {requested_year} workbook does not declare tonnes")

    # Current CORES workbooks use an English annual layout: product totals are
    # rows and January--December are columns.  Parse those explicit total rows
    # before falling back to the older Spanish transposed layout below.
    for index, row in raw.iterrows():
        labels = {_normalise_label(v) for v in row if pd.notna(v)}
        if "products" in labels and any(_normalise_label(v) == "january" for v in raw.iloc[index + 1] if pd.notna(v)):
            month_header = index + 1
            month_names = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
            month_columns = [(col, f"{requested_year:04d}-{month_names[_normalise_label(value)]:02d}-01") for col, value in enumerate(raw.iloc[month_header]) if _normalise_label(value) in month_names]
            targets = {"gasoline": "gasoline", "jet": "jet_fuel", "gasoil": "diesel"}
            found = {}
            for _, data_row in raw.iloc[month_header + 1:].iterrows():
                labels = {
                    _normalise_label(data_row.iloc[0] if len(data_row) > 0 else ""),
                    _normalise_label(data_row.iloc[1] if len(data_row) > 1 else ""),
                }
                for label, product in targets.items():
                    if label in labels:
                        found[product] = data_row
            if set(found) == set(PRODUCTS):
                rows = []
                for col, period in month_columns:
                    values = {product: _spanish_number(found[product].iloc[col]) for product in PRODUCTS}
                    if all(value is None for value in values.values()):
                        continue
                    if any(value is None for value in values.values()):
                        raise ValueError(f"CORES row {period} has an incomplete national total set")
                    rows.append({"date": period, **values})
                return rows
    month_row = None
    month_columns = []
    for index, row in raw.iterrows():
        columns = []
        for col, value in enumerate(row):
            try:
                period = _spain_month(value)
            except ValueError:
                continue
            if period.startswith(f"{requested_year:04d}-"):
                columns.append((col, period))
        if len(columns) >= 1:
            month_row, month_columns = index, columns
            break
    if month_row is None:
        raise ValueError(f"CORES {requested_year} workbook has no monthly date header")
    label_col = None
    for col, value in enumerate(raw.iloc[month_row - 1] if month_row else []):
        if _normalise_label(value) == "productos":
            label_col = col
            break
    if label_col is None:
        # The header is normally one row above dates, but locate the explicit
        # Productos marker if a publisher inserts an explanatory row.
        for index, row in raw.iterrows():
            for col, value in enumerate(row):
                if _normalise_label(value) == "productos":
                    label_col = col
                    break
            if label_col is not None:
                break
    if label_col is None:
        raise ValueError(f"CORES {requested_year} workbook is missing Productos label")
    targets = {"gasolinas": "gasoline", "querosenos": "jet_fuel", "gasoleos": "diesel"}
    target_rows = {}
    for index, row in raw.iloc[month_row + 1:].iterrows():
        label = _normalise_label(row.iloc[label_col])
        if label in targets:
            if label in target_rows:
                raise ValueError(f"CORES {requested_year} has duplicate total row {label}")
            target_rows[label] = index
    if set(targets) != set(target_rows):
        raise ValueError(f"CORES {requested_year} missing national totals: {sorted(set(targets) - set(target_rows))}")
    rows = []
    for col, period in month_columns:
        values = {product: _spanish_number(raw.iloc[target_rows[label], col]) for label, product in targets.items()}
        if all(value is None for value in values.values()):
            continue
        if any(value is None for value in values.values()):
            raise ValueError(f"CORES row {period} has an incomplete national total set")
        rows.append({"date": period, **values})
    return rows


def _spain_downloads(session):
    discovered = {}
    try:
        landing = _get(session, SPAIN_LANDING_URL)
        links = re.findall(r"href\s*=\s*[\"']([^\"']+)[\"']", landing.text, flags=re.I)
        for link in links:
            match = re.search(r"(?:consumo|consumption)[^/]*?(20\d{2})\.xls[x]?", link, flags=re.I)
            if match and "gas" not in link.casefold():
                discovered[int(match.group(1))] = urljoin(SPAIN_LANDING_URL, link.strip())
    except Exception:
        pass
    # CORES keeps a stable annual naming convention even when the landing page
    # changes its HTML. Probe each year explicitly so history remains complete.
    latest_year = max(discovered.keys() | {datetime.now(timezone.utc).year})
    urls = {}
    for year in range(2020, latest_year + 1):
        candidates = []
        if year in discovered:
            candidates.append(discovered[year])
        candidates.extend([
            f"https://www.cores.es/sites/default/files/archivos/estadisticas/est-oil-products-consumption-{year}.xls",
            f"https://www.cores.es/sites/default/files/archivos/estadisticas/est-petroliferos-consumo-{year}.xls",
            f"https://www.cores.es/sites/default/files/archivos/estadisticas/est-oil-products-consumption-{year}.xls",
        ])
        found = None
        for url in dict.fromkeys(candidates):
            response = _get(session, url)
            # CORES occasionally answers a missing static file with an HTML
            # page and HTTP 200; reject that before handing bytes to xlrd.
            if response.content[:4] in (b"\xd0\xcf\x11\xe0", b"PK\x03\x04"):
                found = (url, response.content)
                break
        if found is None:
            raise ValueError(f"CORES annual consumption workbook unavailable for {year}")
        urls[year] = found
    return urls


def fetch_spain(session, today=None):
    """Fetch CORES national Gasolinas, Querosenos and Gasóleos totals."""
    downloads = _spain_downloads(session)
    by_date = {}
    vintages = []
    source_urls = []
    for year in sorted(downloads):
        url, content = downloads[year]
        source_urls.append(url)
        rows = _spain_rows(content, year)
        by_date.update({row["date"]: row for row in rows})  # newest annual vintage wins overlap
        vintages.append(_workbook_vintage(content, f"CORES {year}"))
    rows = [by_date[key] for key in sorted(by_date) if key >= START]
    rows = validate_rows(rows, "tonnes")
    latest = rows[-1]["date"][:7]
    return {
        "rows": rows,
        "source_urls": source_urls,
        "vintage": f"{vintages[-1]} retrieved {_month_limit(today)}; data through {latest}.",
        "validation_note": f"CORES national total rows Gasolinas, Querosenos and Gasóleos; monthly tonnes only (subtypes and cumulative/percentage rows excluded). Final native row {latest}: {rows[-1]['gasoline']}, {rows[-1]['jet_fuel']}, {rows[-1]['diesel']} tonnes.",
    }


# Descriptive alias for callers that use the country name rather than the
# short source key used by the transport-fuel registry.
fetch_united_kingdom = fetch_uk
