"""
Fetch managed-money long/short Commitment of Traders positioning for:
- WTI Light Sweet Crude Oil futures from CFTC Disaggregated Futures Only
- ICE Brent Crude futures from ICE yearly COT history CSV files

Output:
    data/cot_wti_brent_long_short.csv

Run from repo root:
    python scripts/fetch_cot_long_short.py

Notes:
- Positions are converted from contracts to million barrels using 1,000 bbl/contract.
- Dates are COT report dates, normally Tuesday.
- The script keeps long, short, net, open interest, %OI, and week-over-week changes.
"""

from __future__ import annotations

import argparse
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

try:
    import yfinance as yf
except ImportError:  # price overlay is optional
    yf = None


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

CFTC_DISAGG_FUTURES_ONLY_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
WTI_CFTC_CONTRACT_CODE = "067651"
ICE_COT_URL_TEMPLATE = "https://www.ice.com/publicdocs/futures/COTHist{year}.csv"
CONTRACT_SIZE_BBL = 1_000


HTTP_HEADERS = {
    "User-Agent": "macro-data-tracker/1.0 (+https://github.com/ela113arm-arch/macro-data-tracker)",
}


def numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    """Return a numeric series, or NaNs if the source column is absent."""
    if column not in frame.columns:
        return pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Strip BOMs and whitespace from vendor CSV/API column names."""
    frame = frame.copy()
    frame.columns = [
        str(col).replace("\ufeff", "").replace("ï»¿", "").strip()
        for col in frame.columns
    ]
    return frame


def find_column(frame: pd.DataFrame, candidates: set[str]) -> str | None:
    """Case-insensitive exact match for one of several possible vendor column names."""
    candidate_map = {candidate.lower(): candidate for candidate in candidates}
    for col in frame.columns:
        if str(col).lower() in candidate_map:
            return col
    return None


def add_position_metrics(
    frame: pd.DataFrame,
    prefix: str,
    long_contracts: pd.Series,
    short_contracts: pd.Series,
    open_interest: pd.Series | None = None,
) -> pd.DataFrame:
    """Add long/short/net columns in million barrels plus % of open interest."""
    out = frame.copy()
    out[f"{prefix}_mm_long"] = long_contracts / CONTRACT_SIZE_BBL
    out[f"{prefix}_mm_short"] = short_contracts / CONTRACT_SIZE_BBL
    out[f"{prefix}_mm_net"] = out[f"{prefix}_mm_long"] - out[f"{prefix}_mm_short"]

    if open_interest is not None:
        out[f"{prefix}_open_interest"] = open_interest
        out[f"{prefix}_mm_long_pct_oi"] = (long_contracts / open_interest) * 100
        out[f"{prefix}_mm_short_pct_oi"] = (short_contracts / open_interest) * 100
        out[f"{prefix}_mm_net_pct_oi"] = ((long_contracts - short_contracts) / open_interest) * 100

    return out


def fetch_wti_cme(start_date: pd.Timestamp) -> pd.DataFrame:
    """Fetch WTI managed-money long/short data from CFTC Socrata API."""
    params = {
        "$where": (
            f"cftc_contract_market_code = '{WTI_CFTC_CONTRACT_CODE}' "
            "AND futonly_or_combined = 'FutOnly'"
        ),
        "$order": "report_date_as_yyyy_mm_dd ASC",
        "$limit": 5000,
    }

    response = requests.get(
        CFTC_DISAGG_FUTURES_ONLY_URL,
        params=params,
        headers=HTTP_HEADERS,
        timeout=60,
    )
    response.raise_for_status()

    raw = pd.DataFrame(response.json())
    if raw.empty:
        return pd.DataFrame(columns=["date"])

    raw = normalize_columns(raw)
    out = pd.DataFrame({"date": pd.to_datetime(raw["report_date_as_yyyy_mm_dd"], errors="coerce")})
    out = add_position_metrics(
        out,
        "WTI_CME",
        numeric_column(raw, "m_money_positions_long_all"),
        numeric_column(raw, "m_money_positions_short_all"),
        numeric_column(raw, "open_interest_all"),
    )
    out = out.dropna(subset=["date"]).sort_values("date")
    out = out[out["date"] >= start_date]
    return out


def fetch_ice_brent(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    """Fetch ICE Brent managed-money long/short data from yearly ICE COT CSVs."""
    frames: list[pd.DataFrame] = []

    for year in range(start_date.year, end_date.year + 1):
        url = ICE_COT_URL_TEMPLATE.format(year=year)
        try:
            response = requests.get(url, headers=HTTP_HEADERS, timeout=60)
            response.raise_for_status()
            raw = normalize_columns(pd.read_csv(StringIO(response.text)))
        except Exception as exc:
            print(f"ICE Brent {year}: failed: {exc}")
            continue

        market_col = find_column(raw, {"market_and_exchange_names"})
        report_col = find_column(raw, {"report_date_as_yymmdd", "as_of_date_in_form_yymmdd"})
        combined_col = find_column(raw, {"futonly_or_combined"})
        long_col = find_column(raw, {"m_money_positions_long_all"})
        short_col = find_column(raw, {"m_money_positions_short_all"})
        oi_col = find_column(raw, {"open_interest_all"})

        required = [market_col, report_col, combined_col, long_col, short_col]
        if any(col is None for col in required):
            print(f"ICE Brent {year}: missing required COT columns")
            continue

        mask = (
            raw[market_col].astype(str).str.contains("ICE Brent Crude Futures -", case=False, na=False)
            & raw[combined_col].astype(str).eq("FutOnly")
        )
        brent_raw = raw.loc[mask].copy()
        if brent_raw.empty:
            print(f"ICE Brent {year}: no Brent FutOnly rows")
            continue

        out = pd.DataFrame({
            "date": pd.to_datetime(
                brent_raw[report_col].astype(str).str.zfill(6),
                format="%y%m%d",
                errors="coerce",
            )
        })
        out = add_position_metrics(
            out,
            "BRENT_ICE",
            pd.to_numeric(brent_raw[long_col], errors="coerce"),
            pd.to_numeric(brent_raw[short_col], errors="coerce"),
            pd.to_numeric(brent_raw[oi_col], errors="coerce") if oi_col else None,
        )
        out = out.dropna(subset=["date"]).sort_values("date")
        frames.append(out)
        print(f"ICE Brent {year}: {len(out)} observations")
        time.sleep(0.2)

    if not frames:
        return pd.DataFrame(columns=["date"])

    brent = pd.concat(frames, ignore_index=True)
    brent = brent.drop_duplicates(subset=["date"], keep="last")
    brent = brent[brent["date"] >= start_date].sort_values("date")
    return brent


def add_price_overlay(cot: pd.DataFrame) -> pd.DataFrame:
    """Add Tuesday-aligned Brent and WTI close prices when yfinance is available."""
    if yf is None or cot.empty:
        return cot

    out = cot.copy()
    price_symbols = {"brent_close": "BZ=F", "wti_close": "CL=F"}
    start = (out["date"].min() - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    end = (out["date"].max() + pd.Timedelta(days=7)).strftime("%Y-%m-%d")

    for column, symbol in price_symbols.items():
        try:
            hist = yf.Ticker(symbol).history(start=start, end=end, interval="1d")
            if hist.empty:
                continue
            prices = hist[["Close"]].copy()
            prices.index = pd.to_datetime(prices.index).tz_localize(None).normalize()
            aligned = prices.reindex(pd.date_range(prices.index.min(), prices.index.max(), freq="D")).ffill()
            out[column] = out["date"].map(aligned["Close"])
            out[f"{column.replace('_close', '')}_ww"] = out[column].diff()
        except Exception as exc:
            print(f"Price overlay {symbol}: failed: {exc}")
        time.sleep(0.2)

    return out


def build_cot_long_short(start: str, output: Path) -> pd.DataFrame:
    """Build merged WTI + Brent COT long/short CSV."""
    start_date = pd.Timestamp(start)
    end_date = pd.Timestamp.now().normalize()

    print("Fetching WTI CME COT managed-money long/short...")
    wti = fetch_wti_cme(start_date)
    print(f"WTI CME: {len(wti)} observations")

    print("Fetching ICE Brent COT managed-money long/short...")
    brent = fetch_ice_brent(start_date, end_date)
    print(f"ICE Brent: {len(brent)} observations")

    if wti.empty and brent.empty:
        raise RuntimeError("No COT data fetched from CFTC or ICE")

    cot = pd.merge(wti, brent, on="date", how="outer").sort_values("date").reset_index(drop=True)

    for leg in ["long", "short", "net"]:
        cot[f"COMBINED_mm_{leg}"] = cot[[f"WTI_CME_mm_{leg}", f"BRENT_ICE_mm_{leg}"]].sum(axis=1, min_count=1)

    for prefix in ["WTI_CME", "BRENT_ICE", "COMBINED"]:
        for leg in ["long", "short", "net"]:
            col = f"{prefix}_mm_{leg}"
            if col in cot.columns:
                cot[f"{col}_ww"] = cot[col].diff()

    cot = add_price_overlay(cot)
    cot["year"] = cot["date"].dt.year
    cot["week"] = cot["date"].dt.isocalendar().week.astype(int)

    for col in cot.columns:
        if col != "date":
            cot[col] = pd.to_numeric(cot[col], errors="ignore")
    numeric_cols = cot.select_dtypes(include="number").columns
    cot[numeric_cols] = cot[numeric_cols].round(4)

    output.parent.mkdir(parents=True, exist_ok=True)
    cot["date"] = cot["date"].dt.strftime("%Y-%m-%d")
    cot.to_csv(output, index=False)
    print(f"Saved {len(cot)} rows to {output}")
    return cot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch WTI CME and ICE Brent COT long/short data")
    parser.add_argument("--start", default="2023-01-01", help="First COT report date to include, YYYY-MM-DD")
    parser.add_argument(
        "--output",
        default=str(DATA_DIR / "cot_wti_brent_long_short.csv"),
        help="Output CSV path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_cot_long_short(args.start, Path(args.output))


if __name__ == "__main__":
    main()
