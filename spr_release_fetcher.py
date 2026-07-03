"""
SPR release tracker.

Refreshes DOE SPR exchange postings, Energy.gov announcement metadata, award
PDF buyer tables, RFP planned release rows, and EIA weekly SPR inventories.
The generated CSVs back the /spr dashboard and the monthly GitHub report.
"""

from __future__ import annotations

import argparse
import calendar
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin

import pandas as pd
import requests
from pypdf import PdfReader

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - announcement URLs still parse by regex.
    BeautifulSoup = None

try:
    import plotly.graph_objects as go
    from plotly.offline import plot as plotly_plot
except ImportError:  # pragma: no cover - reports still emit tables.
    go = None
    plotly_plot = None

try:
    from config.api_keys import API_KEYS
except ImportError:
    API_KEYS = {
        "EIA": os.environ.get("EIA_API_KEY", ""),
        "FRED": os.environ.get("FRED_API_KEY", ""),
        "BEA": os.environ.get("BEA_API_KEY", ""),
    }


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", ROOT_DIR / "data"))
REPORT_DIR = Path(os.environ.get("SPR_REPORT_DIR", ROOT_DIR / "reports" / "spr"))
DOC_CACHE_DIR = Path(os.environ.get("SPR_DOC_CACHE_DIR", ROOT_DIR / ".tmp" / "spr_docs"))

DOE_POSTING_BASE = "https://www.spr.doe.gov/posting"
DOE_ACTIVE_DOCS_URL = "https://www.spr.doe.gov/doeec/ActiveDocs.htm?type=exchange"
DOE_ARCHIVE_DOCS_URL = "https://www.spr.doe.gov/doeec/ArchiveDocs.htm?type=exchange"
ENERGY_SEARCH_API = "https://www.energy.gov/api/v1/search"
ENERGY_SEARCH_URL = (
    "https://www.energy.gov/search?page=0&sort_by=date"
    "&f%5B0%5D=content_type_rest%3Aarticle"
    "&f%5B1%5D=article_type%3A1&topic=819896"
)
EIA_WSTK_URL = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
EIA_SPR_SERIES = "WCSSTUS1"

ANNOUNCED_VOLUME_MMBL = 172.0
PLAN_START_DATE = pd.Timestamp("2026-03-20")
PLAN_DAYS = 120
PLANNED_DAILY_MMBL = ANNOUNCED_VOLUME_MMBL / PLAN_DAYS
PLANNED_WEEKLY_MMBL = PLANNED_DAILY_MMBL * 7

SITE_NAMES = ["Bayou Choctaw", "Big Hill", "Bryan Mound", "West Hackberry"]
SITE_PATTERN = "|".join(re.escape(site) for site in sorted(SITE_NAMES, key=len, reverse=True))
STREAM_PATTERN = r"(?P<stream>Sweet|Sour)"
MONTH_CODE_RE = re.compile(r"^[A-Z][a-z]{2}-\d{2}$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso() -> str:
    return _utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "MacroDataTracker/1.0 (+https://github.com/ela113arm-arch/macro-data-tracker)",
            "Accept": "application/json,text/html,application/pdf,*/*",
        }
    )
    return session


def _clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = html.unescape(text)
    if "\u00e2" in text or "\u00c2" in text:
        try:
            fixed = text.encode("cp1252").decode("utf-8")
            if "\u00e2" not in fixed:
                text = fixed
        except UnicodeError:
            pass
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _normalize_site_text(text: str) -> str:
    text = text.replace("Bayou C hoctaw", "Bayou Choctaw")
    text = re.sub(r"Bryan\s+Mound", "Bryan Mound", text)
    text = re.sub(r"Big\s+Hill", "Big Hill", text)
    text = re.sub(r"West\s+Hackberry", "West Hackberry", text)
    text = re.sub(r"\*\*", "", text)
    return text


def _barrels_to_mmbbl(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    number = float(re.sub(r"[^0-9.]", "", str(value)))
    return number / 1_000_000.0


def _parse_date(value: Any) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _month_label(month: str) -> str:
    try:
        return pd.Period(month, freq="M").to_timestamp().strftime("%b %Y")
    except Exception:
        return month


def _month_bounds(delivery_period: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    delivery_period = _clean_text(delivery_period)
    if MONTH_CODE_RE.match(delivery_period):
        start = pd.to_datetime(delivery_period, format="%b-%y", errors="coerce")
        if pd.isna(start):
            return None, None
        end = start + pd.offsets.MonthEnd(0)
        return pd.Timestamp(start), pd.Timestamp(end)

    match = re.match(r"([A-Za-z]+)\s*-\s*([A-Za-z]+)\s+(\d{4})", delivery_period)
    if match:
        start_month, end_month, year = match.groups()
        start = pd.to_datetime(f"{start_month} 1 {year}", errors="coerce")
        end_start = pd.to_datetime(f"{end_month} 1 {year}", errors="coerce")
        if pd.isna(start) or pd.isna(end_start):
            return None, None
        end = end_start + pd.offsets.MonthEnd(0)
        return pd.Timestamp(start), pd.Timestamp(end)
    return None, None


def _delivery_month(delivery_period: str) -> str:
    start, _ = _month_bounds(delivery_period)
    return start.strftime("%Y-%m") if start is not None else ""


def _build_document_url(posting: dict[str, Any], folder: dict[str, Any], file_row: dict[str, Any], status: str) -> str:
    parts = [DOE_POSTING_BASE, quote(posting["postingType"], safe="")]
    if status == "archive":
        parts.append("archive")
    parts.extend(
        [
            quote(posting["name"], safe=""),
            quote(folder["name"], safe=""),
            quote(file_row["name"], safe=""),
        ]
    )
    return "/".join(parts)


def _doc_role(filename: str) -> str:
    lowered = filename.lower()
    if "award information" in lowered:
        return "award_information"
    if "request for proposal" in lowered and "exhibit" not in lowered:
        return "request_for_proposal"
    if "exhibit" in lowered:
        return "exhibit"
    if "question" in lowered:
        return "questions_answers"
    if "amendment" in lowered:
        return "amendment"
    return "other"


def fetch_spr_documents() -> pd.DataFrame:
    """Fetch and flatten current/archive DOE SPR exchange document postings."""
    session = _session()
    rows: list[dict[str, Any]] = []
    endpoints = [("current", "Exchange/current"), ("archive", "Exchange/archive")]
    for status, endpoint in endpoints:
        response = session.get(f"{DOE_POSTING_BASE}/{endpoint}", timeout=45)
        response.raise_for_status()
        postings = response.json()
        for posting in postings:
            for folder in posting.get("folders", []):
                for file_row in folder.get("files", []):
                    url = _build_document_url(posting, folder, file_row, status)
                    rows.append(
                        {
                            "posting_name": posting.get("name", ""),
                            "posting_type": posting.get("postingType", ""),
                            "posting_type_value": posting.get("postingTypeValue", ""),
                            "status": status,
                            "parent_folder": (posting.get("parentFolder") or {}).get("folderName", ""),
                            "folder": folder.get("name", ""),
                            "filename": file_row.get("name", ""),
                            "name_only": file_row.get("nameOnly", ""),
                            "time_last_modified": _parse_date(file_row.get("timeLastModified", "")),
                            "doc_role": _doc_role(file_row.get("name", "")),
                            "source_url": url,
                            "active_docs_url": DOE_ACTIVE_DOCS_URL,
                            "archive_docs_url": DOE_ARCHIVE_DOCS_URL,
                            "fetched_at": _utc_iso(),
                        }
                    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["status", "posting_name", "folder", "filename"]).reset_index(drop=True)


def _cache_path_for_url(url: str) -> Path:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", url.split("/")[-1])[:160]
    if not slug.lower().endswith(".pdf"):
        slug += ".pdf"
    digest = str(abs(hash(url)))
    return DOC_CACHE_DIR / f"{digest}_{slug}"


def _download_pdf(url: str) -> Path:
    DOC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path_for_url(url)
    if path.exists() and path.stat().st_size > 1000:
        return path

    response = _session().get(url, timeout=90)
    response.raise_for_status()
    path.write_bytes(response.content)
    return path


def _extract_pdf_text(url: str, max_pages: int = 50) -> str:
    path = _download_pdf(url)
    reader = PdfReader(str(path))
    chunks = []
    for page in reader.pages[:max_pages]:
        chunks.append(page.extract_text() or "")
    return _normalize_site_text("\n".join(chunks))


def _section_c_text(text: str) -> str:
    markers = [m.start() for m in re.finditer(r"C\.1\s+SCOPE OF WORK", text, flags=re.I)]
    if markers:
        start = markers[-1]
    else:
        start = max(text.find("SECTION C"), 0)
    end_match = re.search(r"\n\s*SECTION D\b", text[start:], flags=re.I)
    end = start + end_match.start() if end_match else min(len(text), start + 6000)
    return text[start:end]


def parse_rfp_plan(posting_name: str, source_url: str, text: str) -> tuple[pd.DataFrame, float]:
    """Parse planned outgoing exchange rows from RFP Section C."""
    section = _section_c_text(text)
    total_match = re.search(r"up to\s+([0-9,]+)\s+barrels", section, flags=re.I)
    planned_total_mmbbl = _barrels_to_mmbbl(total_match.group(1)) if total_match else 0.0

    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    new_format = re.compile(
        rf"^(?P<site>{SITE_PATTERN})\s+{STREAM_PATTERN}\s+"
        rf"(?P<volume>[0-9,]+)\s+(?P<delivery>[A-Z][a-z]{{2}}-\d{{2}})\s+"
        rf"(?P<return_site>{SITE_PATTERN})\s+(?P<receipt>[0-9,]+)\*?\s+"
        rf"(?P<minimum_premium>[0-9.]+%)?",
        flags=re.I,
    )
    legacy_format = re.compile(
        rf"^(?P<site>{SITE_PATTERN})\s+{STREAM_PATTERN}\s+"
        rf"(?P<volume>[0-9,]+)\s+(?P<nominal_delivery>[0-9,]+)\s+"
        rf"(?P<delivery>[A-Za-z]+\s*-\s*[A-Za-z]+\s+\d{{4}})"
        rf"(?:\s+(?P<return_site>{SITE_PATTERN})\s+(?P<receipt>[0-9,]+)\*?)?",
        flags=re.I,
    )

    for raw_line in section.splitlines():
        line = _clean_text(_normalize_site_text(raw_line))
        if not any(line.startswith(site) for site in SITE_NAMES):
            continue
        if "barrels per day" in line.lower():
            continue
        match = new_format.match(line) or legacy_format.match(line)
        if not match:
            continue
        data = match.groupdict()
        site = _clean_text(data.get("site", "")).title().replace("Of", "of")
        stream = _clean_text(data.get("stream", "")).title()
        volume_mmbbl = _barrels_to_mmbbl(data.get("volume"))
        delivery_period = _clean_text(data.get("delivery", ""))
        key = (posting_name, site, stream, round(volume_mmbbl, 5), delivery_period)
        if key in seen:
            continue
        seen.add(key)

        start, end = _month_bounds(delivery_period)
        days = int((end - start).days + 1) if start is not None and end is not None else 0
        planned_bpd = volume_mmbbl * 1_000_000 / days if days else 0.0
        rows.append(
            {
                "date": start.strftime("%Y-%m-%d") if start is not None else "",
                "tranche": posting_name,
                "site": site,
                "quality_bucket": stream,
                "stream": stream,
                "volume_mmbbl": volume_mmbbl,
                "delivery_period": delivery_period,
                "delivery_start": start.strftime("%Y-%m-%d") if start is not None else "",
                "delivery_end": end.strftime("%Y-%m-%d") if end is not None else "",
                "delivery_month": start.strftime("%Y-%m") if start is not None else "",
                "delivery_days": days,
                "planned_avg_bpd": planned_bpd,
                "planned_avg_mmbd": planned_bpd / 1_000_000.0,
                "return_site": _clean_text(data.get("return_site", "")),
                "nominal_delivery_rate_mmbbl_per_month": _barrels_to_mmbbl(data.get("nominal_delivery")),
                "nominal_receipt_rate_mmbbl_per_month": _barrels_to_mmbbl(data.get("receipt")),
                "minimum_premium": _clean_text(data.get("minimum_premium", "")),
                "status": "Solicited",
                "source_url": source_url,
                "source_section": "RFP Section C",
                "notes": "Planned outgoing exchange row parsed from DOE RFP Section C.",
            }
        )

    return pd.DataFrame(rows), planned_total_mmbbl


def parse_delivery_rates(posting_name: str, source_url: str, text: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    current_site = ""
    current_stream = ""
    in_rate_table = False
    pending_rows: list[dict[str, Any]] = []
    full_line = re.compile(
        rf"^(?P<site>{SITE_PATTERN})\s+{STREAM_PATTERN}\s+(?P<mode>.+?)\s+(?P<rate>[0-9]{{2,3}},[0-9]{{3}})\s*$",
        flags=re.I,
    )
    standalone_site = re.compile(rf"^(?P<site>{SITE_PATTERN})\s+{STREAM_PATTERN}\s*$", flags=re.I)
    continuation_line = re.compile(r"^(?P<mode>[A-Za-z0-9() /.-]+?)\s+(?P<rate>[0-9]{2,3},[0-9]{3})\s*$")

    def add_row(site: str, stream: str, mode: str, rate: str) -> None:
        rows.append(
            {
                "tranche": posting_name,
                "site": _clean_text(site).title().replace("Of", "of"),
                "stream": _clean_text(stream).title(),
                "mode_of_delivery": (
                    _clean_text(mode)
                    .replace("C ity", "City")
                    .replace("C reek", "Creek")
                    .replace("C harles", "Charles")
                ),
                "barrels_per_day_limit": float(rate.replace(",", "")),
                "source_url": source_url,
                "source_section": "RFP Section B.6(d)",
            }
        )

    def flush_pending(site: str, stream: str) -> None:
        while pending_rows:
            pending = pending_rows.pop(0)
            add_row(site, stream, pending["mode"], pending["rate"])

    for raw_line in text.splitlines():
        line = _clean_text(_normalize_site_text(raw_line))
        header_text = line.replace("C rude", "Crude")
        if "Crude Oil Stream Mode of Delivery Barrels per day Limit" in header_text:
            in_rate_table = True
            current_site = ""
            current_stream = ""
            pending_rows = []
            continue
        if not in_rate_table:
            continue
        if re.search(r"\bB\.7\b|SECTION C\b|C\.1\s+SCOPE", line, flags=re.I):
            break
        if (
            not line
            or line.startswith("REQUEST FOR PROPOSAL")
            or line.startswith("DE-RP")
            or line.startswith("Section B")
            or line.lower().startswith("note:")
            or "barrels per day" in line.lower()
        ):
            continue

        standalone = standalone_site.match(line)
        if standalone:
            current_site = _clean_text(standalone.group("site")).title().replace("Of", "of")
            current_stream = _clean_text(standalone.group("stream")).title()
            flush_pending(current_site, current_stream)
            continue

        match = full_line.match(line)
        if match:
            current_site = _clean_text(match.group("site")).title().replace("Of", "of")
            current_stream = _clean_text(match.group("stream")).title()
            add_row(current_site, current_stream, match.group("mode"), match.group("rate"))
            continue

        continuation = continuation_line.match(line)
        if continuation and current_site and current_stream and not line.lower().startswith("note"):
            add_row(current_site, current_stream, continuation.group("mode"), continuation.group("rate"))
        elif continuation and not current_site:
            pending_rows.append({"mode": continuation.group("mode"), "rate": continuation.group("rate")})
    return pd.DataFrame(rows).drop_duplicates() if rows else pd.DataFrame()


def parse_award_pdf(posting_name: str, source_url: str, text: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    as_of_match = re.search(r"As of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text)
    total_match = re.search(r"exchange of\s+([0-9,]+)\s+barrels", text, flags=re.I)
    announced_match = re.search(r"EXCHANGE OF\s+(?:UP TO\s+)?([0-9.]+)\s+MILLION BARRELS", text, flags=re.I)
    award_date = _parse_date(as_of_match.group(1)) if as_of_match else ""
    award_total_mmbbl = _barrels_to_mmbbl(total_match.group(1)) if total_match else 0.0
    announced_mmbbl = float(announced_match.group(1)) if announced_match else 0.0

    buyer_rows: list[dict[str, Any]] = []
    buyer_pattern = re.compile(r"^(?P<buyer>.+?)\s*[\u2013\u2014-]\s*(?P<volume>[0-9,]+)\s+barrels", flags=re.I)
    for raw_line in text.splitlines():
        line = _clean_text(raw_line)
        match = buyer_pattern.match(line)
        if not match:
            continue
        buyer_rows.append(
            {
                "date": award_date,
                "week": award_date,
                "tranche": posting_name,
                "buyer": _clean_text(match.group("buyer")),
                "volume_mmbbl": _barrels_to_mmbbl(match.group("volume")),
                "status": "Awarded",
                "source_url": source_url,
                "buyer_site_allocation": "Not published in DOE award information",
                "quality_allocation": "Use RFP plan rows; buyer-level sweet/sour split is not published",
            }
        )

    summary = {
        "date": award_date,
        "tranche": posting_name,
        "status": "Awarded",
        "award_total_mmbbl": award_total_mmbbl,
        "award_pdf_announced_up_to_mmbbl": announced_mmbbl,
        "source_url": source_url,
        "buyer_count": len(buyer_rows),
    }
    return pd.DataFrame(buyer_rows), summary


def fetch_energy_announcements(max_pages: int = 2) -> pd.DataFrame:
    """Fetch Energy.gov SPR press-release rows from the same API used by the site."""
    session = _session()
    rows: list[dict[str, Any]] = []
    for page in range(max_pages):
        params = [
            ("keywords", "Strategic Petroleum Reserve"),
            ("page", str(page)),
            ("sort_by", "date"),
            ("f[0]", "content_type_rest:article"),
            ("f[1]", "article_type:1"),
            ("topic", "819896"),
        ]
        response = session.get(ENERGY_SEARCH_API, params=params, timeout=45)
        response.raise_for_status()
        payload = response.json()
        for row in payload.get("rows", []):
            title_html = row.get("title", "")
            href = ""
            if BeautifulSoup is not None:
                tag = BeautifulSoup(title_html, "html.parser").find("a")
                href = tag.get("href", "") if tag else ""
            if not href:
                href_match = re.search(r'href="([^"]+)"', title_html)
                href = href_match.group(1) if href_match else ""
            url = urljoin("https://www.energy.gov", href)
            title = _clean_text(row.get("titleUnion") or title_html)
            summary = _clean_text(row.get("summary", ""))
            rows.append(
                {
                    "date": _parse_date(row.get("date", "")),
                    "source": "Energy.gov",
                    "title": title,
                    "event_type": _infer_event_type(title),
                    "status": _infer_status(title),
                    "volume_mmbbl": _extract_mmbbl_from_text(f"{title} {summary}"),
                    "offices": row.get("offices", ""),
                    "article_type": row.get("articleType", ""),
                    "source_url": url,
                    "search_url": ENERGY_SEARCH_URL,
                    "summary": summary,
                    "raw_id": row.get("id", ""),
                }
            )
        total = payload.get("meta", {}).get("totalResultCount", 0)
        if (page + 1) * 10 >= total:
            break
        time.sleep(0.2)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["source_url"]).sort_values("date", ascending=False)
    return df.reset_index(drop=True)


def _extract_mmbbl_from_text(text: str) -> float:
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(?:-| )?million(?:-| )?barrel", text, flags=re.I)
    return float(match.group(1)) if match else 0.0


def _infer_event_type(title: str) -> str:
    lowered = title.lower()
    if "award" in lowered:
        return "Award"
    if "rfp" in lowered or "request for proposal" in lowered or "initiates" in lowered or "issues" in lowered:
        return "RFP"
    if "release" in lowered:
        return "Announcement"
    return "News"


def _infer_status(title: str) -> str:
    event_type = _infer_event_type(title)
    if event_type == "Award":
        return "Awarded"
    if event_type == "RFP":
        return "Solicited"
    if event_type == "Announcement":
        return "Announced"
    return "Observed"


def build_release_plan(documents: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    documents = fetch_spr_documents() if documents is None else documents
    rfp_docs = documents[
        documents["doc_role"].eq("request_for_proposal")
        & documents["posting_name"].str.startswith("FY26 SPR Oil Release", na=False)
    ].copy()

    plan_frames: list[pd.DataFrame] = []
    rate_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for _, doc in rfp_docs.iterrows():
        text = _extract_pdf_text(doc["source_url"], max_pages=55)
        plan, planned_total = parse_rfp_plan(doc["posting_name"], doc["source_url"], text)
        rates = parse_delivery_rates(doc["posting_name"], doc["source_url"], text)
        if not plan.empty:
            plan_frames.append(plan)
        if not rates.empty:
            rate_frames.append(rates)
        summary_rows.append(
            {
                "tranche": doc["posting_name"],
                "status": doc["status"],
                "planned_total_mmbbl": planned_total if planned_total else float(plan["volume_mmbbl"].sum() if not plan.empty else 0),
                "planned_rows_mmbbl": float(plan["volume_mmbbl"].sum() if not plan.empty else 0),
                "source_url": doc["source_url"],
            }
        )

    plan_df = pd.concat(plan_frames, ignore_index=True) if plan_frames else pd.DataFrame()
    rates_df = pd.concat(rate_frames, ignore_index=True) if rate_frames else pd.DataFrame()
    rfp_summary = pd.DataFrame(summary_rows)
    if not plan_df.empty:
        plan_df = _add_month_columns(plan_df, "date")
    return plan_df, rates_df, rfp_summary


def build_award_data(documents: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    documents = fetch_spr_documents() if documents is None else documents
    award_docs = documents[
        documents["doc_role"].eq("award_information")
        & documents["posting_name"].str.startswith("FY26 SPR Oil Release", na=False)
    ].copy()

    buyer_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for _, doc in award_docs.iterrows():
        text = _extract_pdf_text(doc["source_url"], max_pages=10)
        buyers, summary = parse_award_pdf(doc["posting_name"], doc["source_url"], text)
        summary["posting_status"] = doc["status"]
        summary_rows.append(summary)
        if not buyers.empty:
            buyer_frames.append(buyers)

    buyers_df = pd.concat(buyer_frames, ignore_index=True) if buyer_frames else pd.DataFrame()
    summary_df = pd.DataFrame(summary_rows)
    if not buyers_df.empty:
        buyers_df = _add_month_columns(buyers_df, "date")
        buyers_df = buyers_df.sort_values(["date", "tranche", "buyer"]).reset_index(drop=True)
    if not summary_df.empty:
        summary_df = _add_month_columns(summary_df, "date")
        summary_df = summary_df.sort_values(["date", "tranche"]).reset_index(drop=True)
    return buyers_df, summary_df


def _add_month_columns(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    working = df.copy()
    dates = pd.to_datetime(working[date_col], errors="coerce")
    working["month"] = dates.dt.strftime("%Y-%m")
    working["month_label"] = dates.dt.strftime("%b %Y")
    return working


def build_release_buyers(documents: pd.DataFrame | None = None) -> pd.DataFrame:
    buyers, _ = build_award_data(documents)
    return buyers


def build_release_quality(
    plan: pd.DataFrame | None = None,
    award_summary: pd.DataFrame | None = None,
    documents: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if plan is None:
        plan, _, _ = build_release_plan(documents)
    if award_summary is None:
        _, award_summary = build_award_data(documents)

    rows: list[dict[str, Any]] = []
    if plan is not None and not plan.empty:
        for _, row in plan.iterrows():
            rows.append(
                {
                    "date": row.get("date", ""),
                    "week": row.get("date", ""),
                    "tranche": row.get("tranche", ""),
                    "status": "Solicited",
                    "site": row.get("site", ""),
                    "volume_mmbbl": row.get("volume_mmbbl", 0.0),
                    "quality_bucket": row.get("quality_bucket", ""),
                    "quality_method": "RFP Section C site/stream planned allocation",
                    "source_url": row.get("source_url", ""),
                    "notes": row.get("notes", ""),
                }
            )

    if award_summary is not None and not award_summary.empty and plan is not None and not plan.empty:
        for _, award in award_summary.iterrows():
            tranche_plan = plan[plan["tranche"].eq(award["tranche"])]
            quality_values = sorted(tranche_plan["quality_bucket"].dropna().unique())
            site_values = sorted(tranche_plan["site"].dropna().unique())
            if len(quality_values) == 1:
                quality_bucket = quality_values[0]
                method = "Award PDF buyer total mapped to single-quality RFP tranche"
            else:
                quality_bucket = "Public award split not stated"
                method = "Award PDF does not allocate buyers across the RFP sweet/sour plan rows"
            rows.append(
                {
                    "date": award.get("date", ""),
                    "week": award.get("date", ""),
                    "tranche": award.get("tranche", ""),
                    "status": "Awarded",
                    "site": "; ".join(site_values) if site_values else "",
                    "volume_mmbbl": award.get("award_total_mmbbl", 0.0),
                    "quality_bucket": quality_bucket,
                    "quality_method": method,
                    "source_url": award.get("source_url", ""),
                    "notes": "Buyer-level site and quality allocations are not published in DOE award information.",
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return _add_month_columns(df, "date").sort_values(["date", "tranche", "status"]).reset_index(drop=True)


def build_site_quality(plan: pd.DataFrame | None = None, documents: pd.DataFrame | None = None) -> pd.DataFrame:
    if plan is None:
        plan, _, _ = build_release_plan(documents)
    if plan is None or plan.empty:
        return pd.DataFrame()
    pivot = plan.pivot_table(index="site", columns="quality_bucket", values="volume_mmbbl", aggfunc="sum", fill_value=0)
    pivot = pivot.reset_index()
    for col in ["Sweet", "Sour"]:
        if col not in pivot.columns:
            pivot[col] = 0.0
    pivot["sweet_mmbbl"] = pivot["Sweet"]
    pivot["sour_mmbbl"] = pivot["Sour"]
    pivot["total_mmbbl"] = pivot["sweet_mmbbl"] + pivot["sour_mmbbl"]
    pivot["source_url"] = "DOE RFP Section C planned release rows"
    return pivot[["site", "sweet_mmbbl", "sour_mmbbl", "total_mmbbl", "source_url"]]


def fetch_eia_spr_weekly(start_date: str = "2026-01-01") -> pd.DataFrame:
    api_key = API_KEYS.get("EIA") or os.environ.get("EIA_API_KEY", "")
    if not api_key:
        raise RuntimeError("EIA_API_KEY is required for live SPR weekly inventory refresh.")

    rows: list[dict[str, Any]] = []
    offset = 0
    length = 5000
    session = _session()
    while True:
        params = {
            "api_key": api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": EIA_SPR_SERIES,
            "start": start_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": offset,
            "length": length,
        }
        response = session.get(EIA_WSTK_URL, params=params, timeout=45)
        response.raise_for_status()
        chunk = response.json().get("response", {}).get("data", [])
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < length:
            break
        offset += length
        time.sleep(0.2)

    if not rows:
        raise RuntimeError(f"No EIA rows returned for {EIA_SPR_SERIES}.")
    return parse_eia_spr_rows(rows)


def parse_eia_spr_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["date", "spr_stock_mmbbl"])
    df = df.rename(columns={"period": "date"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["spr_stock_mmbbl"] = pd.to_numeric(df["value"], errors="coerce") / 1000.0
    df = df.dropna(subset=["date", "spr_stock_mmbbl"]).sort_values("date")
    for col in ["series", "series-description", "units"]:
        if col not in df.columns:
            df[col] = ""
    df = df[["date", "spr_stock_mmbbl", "series", "series-description", "units"]].copy()
    return add_inventory_metrics(df)


def add_inventory_metrics(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy().sort_values("date")
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    pre_release = working[working["date"] < PLAN_START_DATE]
    baseline_row = pre_release.iloc[-1] if not pre_release.empty else working.iloc[0]
    baseline_stock = float(baseline_row["spr_stock_mmbbl"])
    baseline_date = pd.Timestamp(baseline_row["date"])

    working["stock_change_mmbbl"] = working["spr_stock_mmbbl"].diff()
    working["weekly_drawdown_mmbbl"] = -working["stock_change_mmbbl"]
    working["weekly_drawdown_bpd"] = working["weekly_drawdown_mmbbl"] * 1_000_000 / 7
    working["eia_week_ending_friday"] = working["date"].dt.strftime("%Y-%m-%d")
    working["eia_publish_date_estimate"] = (working["date"] + pd.Timedelta(days=5)).dt.strftime("%Y-%m-%d")
    working["baseline_date"] = baseline_date.strftime("%Y-%m-%d")
    working["baseline_stock_mmbbl"] = baseline_stock
    working["observed_drawdown_mmbbl"] = baseline_stock - working["spr_stock_mmbbl"]
    working["days_since_plan_start"] = (working["date"] - PLAN_START_DATE).dt.days.clip(lower=0)
    working["planned_drawdown_mmbbl"] = (working["days_since_plan_start"] * PLANNED_DAILY_MMBL).clip(
        upper=ANNOUNCED_VOLUME_MMBL
    )
    working["plan_gap_mmbbl"] = working["observed_drawdown_mmbbl"] - working["planned_drawdown_mmbbl"]
    working["planned_weekly_mmbbl"] = PLANNED_WEEKLY_MMBL
    working = _add_month_columns(working, "date")
    working["date"] = working["date"].dt.strftime("%Y-%m-%d")
    return working


def build_monthly_summary(
    weekly: pd.DataFrame,
    release_quality: pd.DataFrame,
    release_buyers: pd.DataFrame,
    plan: pd.DataFrame | None = None,
    award_summary: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if weekly is not None and not weekly.empty:
        wk = weekly.copy()
        wk["date_dt"] = pd.to_datetime(wk["date"], errors="coerce")
        wk = wk[wk["date_dt"] >= PLAN_START_DATE].copy()
        wk["month"] = wk["date_dt"].dt.strftime("%Y-%m")
        inventory = wk.groupby("month", as_index=False).agg(
            observed_eia_drawdown_mmbbl=("weekly_drawdown_mmbbl", "sum"),
            planned_drawdown_mmbbl=("planned_drawdown_mmbbl", "max"),
            weeks_reported=("date", "count"),
            latest_eia_publish_date_estimate=("eia_publish_date_estimate", "max"),
        )
        month_end = (
            wk.sort_values("date_dt")
            .groupby("month", as_index=False)
            .tail(1)[["month", "date", "spr_stock_mmbbl", "observed_drawdown_mmbbl"]]
            .rename(
                columns={
                    "date": "month_end_date",
                    "spr_stock_mmbbl": "month_end_spr_stock_mmbbl",
                    "observed_drawdown_mmbbl": "cumulative_observed_drawdown_mmbbl",
                }
            )
        )
        frames.append(inventory.merge(month_end, on="month", how="left"))

    if plan is not None and not plan.empty:
        plan_monthly = (
            plan.groupby("delivery_month", as_index=False)["volume_mmbbl"]
            .sum()
            .rename(columns={"delivery_month": "month", "volume_mmbbl": "planned_release_mmbbl"})
        )
        frames.append(plan_monthly)

    if award_summary is not None and not award_summary.empty:
        awards = (
            award_summary.groupby("month", as_index=False)["award_total_mmbbl"]
            .sum()
            .rename(columns={"award_total_mmbbl": "awarded_release_mmbbl"})
        )
        frames.append(awards)

    if release_buyers is not None and not release_buyers.empty:
        buyer_monthly = (
            release_buyers.groupby("month", as_index=False)["volume_mmbbl"]
            .sum()
            .rename(columns={"volume_mmbbl": "buyer_award_mmbbl"})
        )
        frames.append(buyer_monthly)

    if release_quality is not None and not release_quality.empty:
        planned_quality = release_quality[release_quality["status"].eq("Solicited")].pivot_table(
            index="month", columns="quality_bucket", values="volume_mmbbl", aggfunc="sum", fill_value=0
        )
        planned_quality = planned_quality.reset_index()
        for source, dest in [
            ("Sweet", "sweet_mmbbl"),
            ("Sour", "sour_mmbbl"),
            ("Public award split not stated", "public_split_not_stated_mmbbl"),
        ]:
            planned_quality[dest] = planned_quality[source] if source in planned_quality.columns else 0.0
        frames.append(planned_quality[["month", "sweet_mmbbl", "sour_mmbbl", "public_split_not_stated_mmbbl"]])

    if not frames:
        return pd.DataFrame()
    months = sorted(set().union(*[set(frame["month"].dropna()) for frame in frames if "month" in frame.columns]))
    monthly = pd.DataFrame({"month": months})
    for frame in frames:
        monthly = monthly.merge(frame, on="month", how="left")

    numeric_cols = [
        "observed_eia_drawdown_mmbbl",
        "planned_drawdown_mmbbl",
        "weeks_reported",
        "month_end_spr_stock_mmbbl",
        "cumulative_observed_drawdown_mmbbl",
        "planned_release_mmbbl",
        "awarded_release_mmbbl",
        "buyer_award_mmbbl",
        "sweet_mmbbl",
        "sour_mmbbl",
        "public_split_not_stated_mmbbl",
    ]
    for col in numeric_cols:
        if col not in monthly.columns:
            monthly[col] = 0.0
        monthly[col] = pd.to_numeric(monthly[col], errors="coerce").fillna(0)

    monthly["open_not_awarded_mmbbl"] = (monthly["planned_release_mmbbl"] - monthly["awarded_release_mmbbl"]).clip(
        lower=0
    )
    monthly["mixed_split_not_stated_mmbbl"] = 0.0
    monthly["open_rfp_balance_mmbbl"] = monthly["open_not_awarded_mmbbl"]
    monthly["official_release_rows_mmbbl"] = monthly["planned_release_mmbbl"]
    monthly["month_label"] = monthly["month"].map(_month_label)
    monthly["notes"] = (
        "EIA period dates are week-ending Fridays; publish date estimate is the following Wednesday."
    )
    return monthly[
        [
            "month",
            "month_label",
            "planned_release_mmbbl",
            "awarded_release_mmbbl",
            "open_not_awarded_mmbbl",
            "buyer_award_mmbbl",
            "sweet_mmbbl",
            "sour_mmbbl",
            "mixed_split_not_stated_mmbbl",
            "public_split_not_stated_mmbbl",
            "open_rfp_balance_mmbbl",
            "official_release_rows_mmbbl",
            "observed_eia_drawdown_mmbbl",
            "planned_drawdown_mmbbl",
            "month_end_spr_stock_mmbbl",
            "month_end_date",
            "cumulative_observed_drawdown_mmbbl",
            "weeks_reported",
            "latest_eia_publish_date_estimate",
            "notes",
        ]
    ].sort_values("month")


def build_release_summary(
    weekly: pd.DataFrame,
    events: pd.DataFrame,
    site_quality: pd.DataFrame,
    release_quality: pd.DataFrame | None = None,
    plan: pd.DataFrame | None = None,
    award_summary: pd.DataFrame | None = None,
) -> pd.DataFrame:
    latest = weekly.sort_values("date").iloc[-1] if weekly is not None and not weekly.empty else {}
    awarded_mmbbl = float(award_summary["award_total_mmbbl"].sum()) if award_summary is not None and not award_summary.empty else 0
    planned_rows_mmbbl = float(plan["volume_mmbbl"].sum()) if plan is not None and not plan.empty else 0
    active_plan = plan[plan["tranche"].isin(_active_tranches(plan, award_summary))] if plan is not None and not plan.empty else pd.DataFrame()
    active_awards = (
        award_summary[award_summary["tranche"].isin(set(active_plan["tranche"]))]
        if award_summary is not None and not award_summary.empty and not active_plan.empty
        else pd.DataFrame()
    )
    active_open_mmbbl = max(
        0.0,
        float(active_plan["volume_mmbbl"].sum() if not active_plan.empty else 0)
        - float(active_awards["award_total_mmbbl"].sum() if not active_awards.empty else 0),
    )

    quality_awarded = release_quality[release_quality["status"].eq("Awarded")] if release_quality is not None and not release_quality.empty else pd.DataFrame()
    quality_totals = quality_awarded.groupby("quality_bucket")["volume_mmbbl"].sum().to_dict() if not quality_awarded.empty else {}
    rows = [
        ("announced_release_mmbbl", ANNOUNCED_VOLUME_MMBL, "MMbbl", "Announced release target"),
        ("planned_daily_mmbbl", PLANNED_DAILY_MMBL, "MMbbl/day", "Implied announced pace over 120 days"),
        ("planned_weekly_mmbbl", PLANNED_WEEKLY_MMBL, "MMbbl/week", "Implied announced weekly pace"),
        ("rfp_planned_rows_mmbbl", planned_rows_mmbbl, "MMbbl", "Total DOE RFP planned rows; can exceed target because RFPs solicit replacement/remaining barrels"),
        ("awarded_mmbbl", awarded_mmbbl, "MMbbl", "Total awarded in DOE award PDFs"),
        ("remaining_vs_announced_mmbbl", ANNOUNCED_VOLUME_MMBL - awarded_mmbbl, "MMbbl", "Announced target minus public award PDFs"),
        ("active_open_rfp_balance_mmbbl", active_open_mmbbl, "MMbbl", "Current active RFP rows less awards in that active tranche"),
        ("latest_spr_stock_mmbbl", float(latest.get("spr_stock_mmbbl", 0) or 0), "MMbbl", "Latest EIA SPR stock"),
        ("observed_eia_drawdown_mmbbl", float(latest.get("observed_drawdown_mmbbl", 0) or 0), "MMbbl", "Observed EIA drawdown since pre-release baseline"),
        ("latest_weekly_drawdown_mmbbl", float(latest.get("weekly_drawdown_mmbbl", 0) or 0), "MMbbl/week", "Latest EIA weekly drawdown"),
        ("latest_weekly_drawdown_bpd", float(latest.get("weekly_drawdown_bpd", 0) or 0), "b/d", "Latest EIA weekly drawdown rate"),
        ("released_sweet_mmbbl", float(quality_totals.get("Sweet", 0)), "MMbbl", "Awarded volume mapped to single-quality sweet tranches"),
        ("released_sour_mmbbl", float(quality_totals.get("Sour", 0)), "MMbbl", "Awarded volume mapped to single-quality sour tranches"),
        (
            "released_public_split_not_stated_mmbbl",
            float(quality_totals.get("Public award split not stated", 0)),
            "MMbbl",
            "Awarded volume where buyer-level sweet/sour split is not public",
        ),
        ("planned_sweet_rows_mmbbl", float(site_quality["sweet_mmbbl"].sum()) if site_quality is not None and not site_quality.empty else 0, "MMbbl", "RFP planned sweet rows"),
        ("planned_sour_rows_mmbbl", float(site_quality["sour_mmbbl"].sum()) if site_quality is not None and not site_quality.empty else 0, "MMbbl", "RFP planned sour rows"),
    ]
    summary = pd.DataFrame(
        [{"metric": metric, "value": value, "unit": unit, "label": label} for metric, value, unit, label in rows]
    )
    summary["as_of_date"] = latest.get("date", "")
    summary["eia_publish_date_estimate"] = latest.get("eia_publish_date_estimate", "")
    summary["refreshed_at"] = _utc_iso()
    return summary


def _active_tranches(plan: pd.DataFrame | None, award_summary: pd.DataFrame | None) -> set[str]:
    if plan is None or plan.empty:
        return set()
    # Active tranches are inferred from plan rows whose source URL lacks /archive/.
    active = set(plan.loc[~plan["source_url"].str.contains("/archive/", na=False), "tranche"])
    if active:
        return active
    return {str(plan.sort_values("date").iloc[-1]["tranche"])}


def build_release_events(
    announcements: pd.DataFrame | None = None,
    plan: pd.DataFrame | None = None,
    award_summary: pd.DataFrame | None = None,
    enrich_pages: bool = True,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if announcements is None:
        announcements = fetch_energy_announcements()
    if announcements is not None and not announcements.empty:
        for _, row in announcements.iterrows():
            rows.append(
                {
                    "date": row.get("date", ""),
                    "event_type": row.get("event_type", "News"),
                    "status": row.get("status", "Observed"),
                    "source": "Energy.gov",
                    "title": row.get("title", ""),
                    "volume_mmbbl": row.get("volume_mmbbl", 0.0),
                    "returned_volume_mmbbl": "",
                    "delivery_start": "",
                    "delivery_end": "",
                    "pace_note": "",
                    "sites": "",
                    "quality": "",
                    "counterparties": "",
                    "source_url": row.get("source_url", ""),
                    "award_pdf_url": "",
                    "notes": row.get("summary", ""),
                }
            )
    if plan is not None and not plan.empty:
        for tranche, group in plan.groupby("tranche"):
            rows.append(
                {
                    "date": group["date"].min(),
                    "event_type": "RFP",
                    "status": "Solicited",
                    "source": "DOE SPR",
                    "title": tranche,
                    "volume_mmbbl": group["volume_mmbbl"].sum(),
                    "returned_volume_mmbbl": "",
                    "delivery_start": group["delivery_start"].min(),
                    "delivery_end": group["delivery_end"].max(),
                    "pace_note": "Planned average rates are in spr_release_plan.csv.",
                    "sites": "; ".join(sorted(group["site"].dropna().unique())),
                    "quality": "; ".join(sorted(group["quality_bucket"].dropna().unique())),
                    "counterparties": "",
                    "source_url": group["source_url"].iloc[0],
                    "award_pdf_url": "",
                    "notes": "Parsed from DOE RFP Section C.",
                }
            )
    if award_summary is not None and not award_summary.empty:
        for _, award in award_summary.iterrows():
            rows.append(
                {
                    "date": award.get("date", ""),
                    "event_type": "Award",
                    "status": "Awarded",
                    "source": "DOE SPR",
                    "title": f"{award.get('tranche', '')} Award Information",
                    "volume_mmbbl": award.get("award_total_mmbbl", 0.0),
                    "returned_volume_mmbbl": "",
                    "delivery_start": "",
                    "delivery_end": "",
                    "pace_note": "Buyer totals are in spr_release_buyers.csv.",
                    "sites": "",
                    "quality": "Buyer-level split not published unless tranche is single quality.",
                    "counterparties": "",
                    "source_url": award.get("source_url", ""),
                    "award_pdf_url": award.get("source_url", ""),
                    "notes": f"{int(award.get('buyer_count', 0))} buyer rows parsed from DOE award PDF.",
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return _add_month_columns(df, "date").sort_values("date", ascending=False).reset_index(drop=True)


def build_news(events: pd.DataFrame) -> pd.DataFrame:
    if events is None or events.empty:
        return pd.DataFrame()
    return events[
        [
            "date",
            "source",
            "title",
            "event_type",
            "status",
            "volume_mmbbl",
            "sites",
            "quality",
            "source_url",
            "award_pdf_url",
            "notes",
        ]
    ].sort_values("date", ascending=False)


def _format_number(value: Any, digits: int = 1) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return str(value)


def _df_to_markdown(df: pd.DataFrame, cols: list[str], limit: int = 12) -> str:
    if df is None or df.empty:
        return "_No rows._"
    display = df[cols].head(limit).copy()
    try:
        return display.to_markdown(index=False)
    except ImportError:
        headers = [str(col) for col in display.columns]
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        for _, row in display.iterrows():
            values = [str(row[col]).replace("|", "\\|") for col in display.columns]
            lines.append("| " + " | ".join(values) + " |")
        return "\n".join(lines)


def _df_to_html_table(df: pd.DataFrame, cols: list[str], limit: int = 12) -> str:
    if df is None or df.empty:
        return "<p class='note'>No rows.</p>"
    display = df[cols].head(limit).copy()
    return display.to_html(index=False, escape=True)


def generate_monthly_report(
    summary: pd.DataFrame,
    monthly: pd.DataFrame,
    weekly: pd.DataFrame,
    plan: pd.DataFrame,
    buyers: pd.DataFrame,
    award_summary: pd.DataFrame,
    announcements: pd.DataFrame,
    report_month: str | None = None,
    report_dir: Path = REPORT_DIR,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    if not report_month:
        if weekly is not None and not weekly.empty:
            report_month = weekly.sort_values("date").iloc[-1]["month"]
        else:
            report_month = _utc_now().strftime("%Y-%m")

    month_row = monthly[monthly["month"].eq(report_month)].tail(1) if monthly is not None and not monthly.empty else pd.DataFrame()
    latest = weekly.sort_values("date").iloc[-1] if weekly is not None and not weekly.empty else {}
    metrics = dict(zip(summary["metric"], summary["value"])) if summary is not None and not summary.empty else {}
    label = _month_label(report_month)
    refreshed_text = _utc_now().strftime("%Y-%m-%d %H:%M UTC")
    headline_text = (
        f"Public DOE award PDFs total {_format_number(metrics.get('awarded_mmbbl', 0), 2)} MMbbl "
        f"against the {_format_number(ANNOUNCED_VOLUME_MMBL, 1)} MMbbl announced target. "
        f"The latest EIA SPR inventory is {_format_number(latest.get('spr_stock_mmbbl', 0), 1)} MMbbl "
        f"for week ending {latest.get('eia_week_ending_friday', '')}, which EIA would normally publish "
        f"around {latest.get('eia_publish_date_estimate', '')}."
    )
    if not month_row.empty:
        row = month_row.iloc[0]
        current_month_text = (
            f"{label}: planned RFP rows {_format_number(row.get('planned_release_mmbbl', 0), 2)} MMbbl, "
            f"awards {_format_number(row.get('awarded_release_mmbbl', 0), 2)} MMbbl, "
            f"EIA observed drawdown {_format_number(row.get('observed_eia_drawdown_mmbbl', 0), 2)} MMbbl."
        )
    else:
        current_month_text = "No monthly row is available for this report month."
    planned_display = plan.sort_values(["delivery_start", "tranche", "site"]) if plan is not None and not plan.empty else plan
    buyer_display = (
        buyers.sort_values(["date", "tranche", "volume_mmbbl"], ascending=[False, True, False])
        if buyers is not None and not buyers.empty
        else buyers
    )
    announcement_display = (
        announcements.sort_values("date", ascending=False) if announcements is not None and not announcements.empty else announcements
    )

    md_lines = [
        f"# SPR release tracker - {label}",
        "",
        f"Refreshed: {refreshed_text}",
        "",
        "## Headline",
        "",
        headline_text,
        "",
        "## Current Month",
        "",
    ]
    md_lines.append(current_month_text)

    md_lines.extend(
        [
            "",
            "## Planned Rows",
            "",
            _df_to_markdown(
                planned_display,
                ["tranche", "site", "quality_bucket", "volume_mmbbl", "delivery_period", "planned_avg_bpd"],
                20,
            ),
            "",
            "## Awarded Buyers",
            "",
            _df_to_markdown(
                buyer_display,
                ["date", "tranche", "buyer", "volume_mmbbl"],
                20,
            ),
            "",
            "## Latest Announcements",
            "",
            _df_to_markdown(
                announcement_display,
                ["date", "event_type", "title", "volume_mmbbl", "source_url"],
                10,
            ),
            "",
            "## EIA Timing Note",
            "",
            "EIA weekly SPR stock periods are week-ending Fridays. This tracker adds `eia_publish_date_estimate`, the following Wednesday, so monthly math can distinguish inventory as-of dates from release dates.",
            "",
        ]
    )
    markdown = "\n".join(md_lines)

    month_slug = report_month.replace("-", "_")
    md_path = report_dir / f"spr_release_report_{month_slug}.md"
    latest_md_path = report_dir / "spr_release_report_latest.md"
    md_path.write_text(markdown, encoding="utf-8")
    latest_md_path.write_text(markdown, encoding="utf-8")

    chart_div = ""
    if go is not None and plotly_plot is not None and weekly is not None and not weekly.empty:
        wk = weekly.copy()
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=wk["date"],
                y=wk["spr_stock_mmbbl"],
                mode="lines+markers",
                name="SPR stock (MMbbl)",
                line=dict(color="#1f4e79", width=2),
            )
        )
        fig.add_trace(
            go.Bar(
                x=wk["date"],
                y=wk["weekly_drawdown_mmbbl"],
                name="Weekly drawdown (MMbbl)",
                marker_color="#b45f06",
                yaxis="y2",
                opacity=0.45,
            )
        )
        fig.update_layout(
            template="plotly_white",
            title="EIA SPR inventory and weekly drawdown",
            xaxis_title="EIA week-ending Friday",
            yaxis=dict(title="SPR stock (MMbbl)"),
            yaxis2=dict(title="Weekly drawdown (MMbbl)", overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(l=60, r=60, t=70, b=50),
        )
        chart_div = plotly_plot(fig, include_plotlyjs="cdn", output_type="div")

    html_sections = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        f"<title>SPR release tracker - {html.escape(label)}</title>",
        "<style>body{font-family:Arial,sans-serif;max-width:1180px;margin:32px auto;color:#111;line-height:1.45}"
        "table{border-collapse:collapse;width:100%;font-size:13px;margin:16px 0}"
        "th,td{border:1px solid #ddd;padding:6px;text-align:left;vertical-align:top}"
        "th{background:#f3f5f7}.note{color:#555}.source{max-width:260px;overflow-wrap:anywhere}</style></head><body>",
        f"<h1>SPR release tracker - {html.escape(label)}</h1>",
        f"<p class='note'>Refreshed: {html.escape(refreshed_text)}</p>",
        "<h2>Headline</h2>",
        f"<p>{html.escape(headline_text)}</p>",
        "<h2>Current Month</h2>",
        f"<p>{html.escape(current_month_text)}</p>",
        "<h2>Planned Rows</h2>",
        _df_to_html_table(
            planned_display,
            ["tranche", "site", "quality_bucket", "volume_mmbbl", "delivery_period", "planned_avg_bpd"],
            20,
        ),
        "<h2>Awarded Buyers</h2>",
        _df_to_html_table(buyer_display, ["date", "tranche", "buyer", "volume_mmbbl"], 20),
        "<h2>Latest Announcements</h2>",
        _df_to_html_table(announcement_display, ["date", "event_type", "title", "volume_mmbbl", "source_url"], 10),
        "<h2>EIA Timing Note</h2>",
        "<p>EIA weekly SPR stock periods are week-ending Fridays. This tracker adds "
        "<code>eia_publish_date_estimate</code>, the following Wednesday, so monthly math can distinguish "
        "inventory as-of dates from release dates.</p>",
        chart_div,
        "</body></html>",
    ]
    html_doc = "\n".join(html_sections)
    html_path = report_dir / f"spr_release_report_{month_slug}.html"
    latest_html_path = report_dir / "spr_release_report_latest.html"
    html_path.write_text(html_doc, encoding="utf-8")
    latest_html_path.write_text(html_doc, encoding="utf-8")

    return {"markdown": md_path, "latest_markdown": latest_md_path, "html": html_path, "latest_html": latest_html_path}


def fetch_all(enrich_pages: bool = True, report_month: str | None = None) -> dict[str, int]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("SPR Release Tracker")
    print(f"Timestamp: {_utc_iso()}")
    print("=" * 60)

    documents = fetch_spr_documents()
    announcements = fetch_energy_announcements()
    plan, delivery_rates, rfp_summary = build_release_plan(documents)
    buyers, award_summary = build_award_data(documents)
    release_quality = build_release_quality(plan, award_summary)
    site_quality = build_site_quality(plan)
    weekly = fetch_eia_spr_weekly()
    events = build_release_events(announcements, plan, award_summary, enrich_pages=enrich_pages)
    monthly = build_monthly_summary(weekly, release_quality, buyers, plan, award_summary)
    summary = build_release_summary(weekly, events, site_quality, release_quality, plan, award_summary)
    news = build_news(events)
    reports = generate_monthly_report(summary, monthly, weekly, plan, buyers, award_summary, announcements, report_month)

    outputs = {
        "spr_documents.csv": documents,
        "spr_announcements.csv": announcements,
        "spr_release_plan.csv": plan,
        "spr_delivery_rates.csv": delivery_rates,
        "spr_rfp_summary.csv": rfp_summary,
        "spr_award_summary.csv": award_summary,
        "spr_release_events.csv": events,
        "spr_site_quality.csv": site_quality,
        "spr_release_quality.csv": release_quality,
        "spr_release_buyers.csv": buyers,
        "spr_weekly_inventory.csv": weekly,
        "spr_monthly_summary.csv": monthly,
        "spr_release_summary.csv": summary,
        "spr_news.csv": news,
    }
    row_counts: dict[str, int] = {}
    for filename, df in outputs.items():
        df.to_csv(DATA_DIR / filename, index=False)
        row_counts[filename] = len(df)
        print(f"Saved {len(df):>4} rows to data/{filename}")

    for label, path in reports.items():
        print(f"Report {label}: {path}")

    print("=" * 60)
    print("Done")
    print("=" * 60)
    return row_counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh SPR release tracking data and monthly report.")
    parser.add_argument("--report-month", help="YYYY-MM report month. Defaults to latest EIA week month.")
    parser.add_argument("--no-page-enrich", action="store_true", help="Reserved for compatibility.")
    args = parser.parse_args()
    fetch_all(enrich_pages=not args.no_page_enrich, report_month=args.report_month)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"SPR refresh failed: {exc}", file=sys.stderr)
        raise
