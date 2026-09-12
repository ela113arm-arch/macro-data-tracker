"""EIA United States and Statistics Canada transport-fuel adapters."""

from datetime import datetime, timezone
import io
import re
import zipfile

import pandas as pd

from . import PRODUCTS, START, iso_month, parse_number, validate_rows

EIA_URL = "https://www.eia.gov/dnav/pet/xls/PET_CONS_PSUP_DC_NUS_MBBLPD_M.xls"
CANADA_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/25100081-eng.zip"


def _eia_series_columns(raw):
    """Resolve documented EIA source-key variants without fuzzy matching."""
    keys = {str(v).strip(): i for i, v in enumerate(raw.iloc[1]) if pd.notna(v)}
    needed = {"gasoline": "MGFUPUS2", "jet_fuel": "MKJUPUS2", "diesel": "MDIUPUS2"}
    found = {}
    for product, central in needed.items():
        matches = [
            (key, index) for key, index in keys.items()
            if re.search(rf"(?:^|\\.){re.escape(central)}(?:\\.M)?$", key, flags=re.I)
        ]
        if len(matches) != 1:
            raise ValueError(f"EIA workbook requires one exact {central} source key; found {len(matches)}")
        found[product] = matches[0][1]
    return found


def _response(session, url):
    response = session.get(url, timeout=90)
    response.raise_for_status()
    return response


def fetch_united_states(session, today=None):
    response = _response(session, EIA_URL)
    workbook = pd.ExcelFile(io.BytesIO(response.content))
    raw = pd.read_excel(workbook, sheet_name="Data 1", header=None)
    columns = _eia_series_columns(raw)
    rows = []
    # Row 2 is the human-readable header; observations start below it.
    for _, record in raw.iloc[3:].iterrows():
        if pd.isna(record.iloc[0]) or str(record.iloc[0]).strip().lower() in {"nat", "nan"}:
            continue
        period = iso_month(record.iloc[0])
        # The EIA workbook starts in 1936, while the dashboard contract starts
        # in July 2020. Older rows may legitimately have missing series values.
        if period < START:
            continue
        values = {product: parse_number(record.iloc[index]) for product, index in columns.items()}
        if all(value is None for value in values.values()):
            continue
        if any(value is None for value in values.values()):
            raise ValueError(f"EIA row {period} has an incomplete product set")
        rows.append({"date": period, **values})
    rows = validate_rows(rows, "thousand b/d")
    latest = rows[-1]["date"][:7]
    return {
        "rows": rows,
        "native_unit": "thousand b/d",
        "source_urls": [EIA_URL],
        "vintage": f"EIA Product Supplied workbook retrieved {datetime.now(timezone.utc).date().isoformat()}; data through {latest}.",
        "validation_note": f"Data 1 source keys MGFUPUS2, MKJUPUS2 and MDIUPUS2; final row {latest} checked after workbook parsing.",
    }


def fetch_canada(session, today=None):
    response = _response(session, CANADA_URL)
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        names = [name for name in archive.namelist() if name.endswith(".csv") and "MetaData" not in name]
        if not names:
            raise ValueError("Statistics Canada download does not contain the data CSV")
        raw = pd.read_csv(archive.open(names[0]))
    required = {"REF_DATE", "GEO", "Supply and disposition", "Products", "VALUE", "UOM", "SCALAR_FACTOR"}
    if not required.issubset(raw.columns):
        raise ValueError(f"Statistics Canada CSV missing columns: {sorted(required - set(raw.columns))}")
    products = {
        "Finished motor gasoline": "gasoline",
        "Kerosene-type jet fuel": "jet_fuel",
        "Distillate fuel oil": "diesel",
    }
    subset = raw[
        (raw["GEO"] == "Canada")
        & (raw["Supply and disposition"] == "Products supplied, disposition")
        & raw["Products"].isin(products)
    ].copy()
    if set(subset["UOM"].dropna()) != {"Cubic metres"}:
        raise ValueError("Statistics Canada unit changed; expected Cubic metres")
    if set(subset["SCALAR_FACTOR"].dropna()) != {"units"}:
        raise ValueError("Statistics Canada scalar factor changed; expected units")
    if "STATUS" in subset:
        # A status flag with no value means the publisher suppressed the
        # observation. It must not be converted to zero or carried forward.
        subset = subset[subset["VALUE"].notna()].copy()
    subset["product"] = subset["Products"].map(products)
    subset["date"] = subset["REF_DATE"].map(iso_month)
    if subset.duplicated(["date", "product"]).any():
        raise ValueError("Statistics Canada extract has duplicate country/product/month rows")
    pivot = subset.pivot(index="date", columns="product", values="VALUE").reset_index()
    rows = []
    for record in pivot.to_dict("records"):
        rows.append({"date": record["date"], **{product: record.get(product) for product in PRODUCTS}})
    rows = [row for row in rows if row["date"] >= START]
    rows = validate_rows(rows, "m3")
    latest = rows[-1]["date"][:7]
    return {
        "rows": rows,
        "native_unit": "m3",
        "source_urls": [CANADA_URL],
        "vintage": f"Statistics Canada table 25-10-0081-01 retrieved {datetime.now(timezone.utc).date().isoformat()}; data through {latest}.",
        "validation_note": f"Canada / Products supplied, disposition / three mapped products; final row {latest} checked after CSV pivot.",
    }
