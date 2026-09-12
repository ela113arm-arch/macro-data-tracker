"""Fetch and publish the ten official transport-fuel source series.

This is the source layer for the existing ``update_transport_fuels.py``
publisher. Every country is attempted independently; a source error is
recorded in the manifest and leaves that country's last good bundle intact.
"""

from datetime import date, datetime, timezone
import argparse
import json
from pathlib import Path
import sys
import tempfile

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Running this file directly (as GitHub Actions does) puts ``scripts/`` first
# on sys.path; add the repository root so the adapter package is importable.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from transport_fuel_sources import PRODUCTS, csv_bytes, iso_month, validate_rows
from transport_fuel_sources.us_canada import fetch_canada, fetch_united_states
from scripts.update_transport_fuels import apply_manifest

try:
    from transport_fuel_sources.asia import fetch_india, fetch_japan, fetch_south_korea
except ImportError as exc:  # Keep US/Canada usable while regional adapters land.
    _asia_error = exc
    def _missing_asia(*_args, **_kwargs):
        raise RuntimeError(f"Asia adapter unavailable: {_asia_error}")
    fetch_india = fetch_japan = fetch_south_korea = _missing_asia

try:
    from transport_fuel_sources.europe import fetch_spain, fetch_united_kingdom
except ImportError as exc:
    _europe_error = exc
    def _missing_europe(*_args, **_kwargs):
        raise RuntimeError(f"Europe adapter unavailable: {_europe_error}")
    fetch_spain = fetch_united_kingdom = _missing_europe

try:
    from transport_fuel_sources.other import fetch_australia, fetch_brazil, fetch_mexico
except ImportError as exc:
    _other_error = exc
    def _missing_other(*_args, **_kwargs):
        raise RuntimeError(f"Other-country adapter unavailable: {_other_error}")
    fetch_australia = fetch_brazil = fetch_mexico = _missing_other

COUNTRIES = {
    "United States": fetch_united_states,
    "India": fetch_india,
    "Brazil": fetch_brazil,
    "Japan": fetch_japan,
    "Mexico": fetch_mexico,
    "United Kingdom": fetch_united_kingdom,
    "Spain": fetch_spain,
    "Australia": fetch_australia,
    "South Korea": fetch_south_korea,
    "Canada": fetch_canada,
}


def _session():
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": "macro-data-tracker transport-fuel ETL (official-source retrieval)"})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _complete_rows(result, today):
    if not isinstance(result, dict) or not isinstance(result.get("rows"), list):
        raise ValueError("adapter did not return rows")
    rows = []
    current_month = today.replace(day=1)
    for row in result["rows"]:
        clean = {"date": iso_month(row["date"]), **{product: row.get(product) for product in PRODUCTS}}
        period = date.fromisoformat(clean["date"])
        if period > current_month:
            raise ValueError(f"adapter returned future month {clean['date']}")
        if period == current_month:
            if any(clean[product] is not None for product in PRODUCTS):
                raise ValueError(f"adapter returned unpublished current month {clean['date']}")
            continue
        rows.append(clean)
    unit = result.get("native_unit") or next((row.get("native_unit") for row in result["rows"] if row.get("native_unit")), None)
    if not unit:
        raise ValueError("adapter omitted native_unit")
    return validate_rows(rows, unit), unit


def fetch_transport_fuels_dashboard(root=ROOT, today=None):
    """Run all adapters and publish their manifest through the existing writer."""
    today = today or datetime.now(timezone.utc).date()
    session = _session()
    manifest = {}
    with tempfile.TemporaryDirectory(prefix="transport-fuels-") as tmp:
        work = Path(tmp)
        for country, adapter in COUNTRIES.items():
            try:
                result = adapter(session, today=today)
                rows, unit = _complete_rows(result, today)
                filename = country.lower().replace(" ", "_") + ".csv"
                (work / filename).write_bytes(csv_bytes(rows))
                manifest[country] = {
                    "status": "ok",
                    "csv": filename,
                    "source_urls": result.get("source_urls", []),
                    "vintage": result["vintage"],
                    "validation_note": result["validation_note"],
                }
            except Exception as exc:
                manifest[country] = {"status": "error", "message": f"{type(exc).__name__}: {str(exc)[:450]}"}
        manifest_path = work / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
        failures = apply_manifest(manifest_path, root=root, now=datetime.now(timezone.utc))
    return {"failed_countries": failures, "published_countries": len(COUNTRIES) - len(failures), "manifest": manifest}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    print(json.dumps(fetch_transport_fuels_dashboard(args.root), ensure_ascii=False))
