"""Primary-source adapters for the international transport-fuel ETL.

Adapters return a small, deliberately boring contract so a failure in one
publisher cannot prevent the other countries from refreshing.
"""

from calendar import monthrange
from datetime import date
import csv
import io
import math
import re
from typing import Iterable

PRODUCTS = ("gasoline", "jet_fuel", "diesel")
START = "2020-07-01"


def parse_number(value):
    """Parse common agency numeric cells, preserving decimals and blanks."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"Unrecognised numeric value: {value!r}")
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    text = str(value).strip().replace("\u00a0", "").replace("\u202f", "")
    if not text or text.casefold() in {"-", "…", "...", "na", "n/a", "nan", "nat", "none"}:
        return None
    # The shared parser accepts the two formats used by the official XLS/XLSX
    # extracts: plain decimals and comma-grouped decimals. Locale-specific
    # decimal commas are handled by the Spain adapter, where the source unit
    # and formatting are known. Reject annotations and malformed grouping so a
    # changed header or footnote cannot become a different number silently.
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", text):
        number = float(text)
    elif re.fullmatch(r"[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?", text):
        number = float(text.replace(",", ""))
    else:
        raise ValueError(f"Unrecognised numeric value: {value!r}")
    return number if math.isfinite(number) else None


def iso_month(value):
    """Normalize year/month labels to an ISO first-of-month string."""
    if isinstance(value, date):
        return value.replace(day=1).date().isoformat() if hasattr(value, "date") else value.replace(day=1).isoformat()
    text = str(value).strip()
    m = re.search(r"(20\d{2})[-/.](\d{1,2})", text)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-01"
    m = re.search(r"(\d{1,2})[-/.](20\d{2})", text)
    if m:
        return f"{int(m.group(2)):04d}-{int(m.group(1)):02d}-01"
    raise ValueError(f"Unrecognised month: {value!r}")


def validate_rows(rows: Iterable[dict], unit: str) -> list[dict]:
    """Validate an adapter's source-level rows before they are published."""
    rows = list(rows)
    rows.sort(key=lambda r: r["date"])
    expected = START
    out = []
    for row in rows:
        if row["date"] != expected:
            raise ValueError(f"expected {expected}, got {row['date']} (gap/duplicate/out of order)")
        clean = {"date": row["date"], "native_unit": unit}
        for product in PRODUCTS:
            value = parse_number(row.get(product))
            if value is None or not math.isfinite(value) or value < 0:
                raise ValueError(f"missing or invalid {product} for {row['date']}")
            clean[product] = value
        out.append(clean)
        y, m = map(int, expected[:7].split("-"))
        expected = f"{y + (m == 12):04d}-{(m % 12) + 1:02d}-01"
    if len(out) < 13:
        raise ValueError("source contains fewer than 13 months")
    return out


def csv_bytes(rows: Iterable[dict]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=["date", *PRODUCTS, "native_unit"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def month_days(period: str) -> int:
    return monthrange(int(period[:4]), int(period[5:7]))[1]
