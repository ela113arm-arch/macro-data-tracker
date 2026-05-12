"""
SPR Release Fetcher

Builds the Strategic Petroleum Reserve release tracker datasets used by the
standalone SPR dashboard. The official release plan and public DOE/IEA facts are
kept as structured source rows; EIA WCSSTUS1 provides the observed weekly stock
path.
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - dashboard still works without HTML parsing.
    BeautifulSoup = None

try:
    from config.api_keys import API_KEYS
except ImportError:
    API_KEYS = {
        "EIA": os.environ.get("EIA_API_KEY", ""),
        "FRED": os.environ.get("FRED_API_KEY", ""),
        "BEA": os.environ.get("BEA_API_KEY", ""),
    }


DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).parent / "data"))
EIA_SPR_SERIES = "WCSSTUS1"
EIA_WSTK_URL = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
PLAN_START_DATE = pd.Timestamp("2026-03-20")
ANNOUNCED_VOLUME_MMBL = 172.0
IEA_US_CONTRIBUTION_MMBL = 172.2
RETURN_EXPECTED_MMBL = 200.0
PLAN_DAYS = 120
PLANNED_DAILY_MMBL = ANNOUNCED_VOLUME_MMBL / PLAN_DAYS
PLANNED_WEEKLY_MMBL = PLANNED_DAILY_MMBL * 7


OFFICIAL_SOURCE_EVENTS = [
    {
        "date": "2026-03-11",
        "event_type": "Announcement",
        "status": "Announced",
        "source": "DOE",
        "title": "United States to Release 172 Million Barrels of Oil from the Strategic Petroleum Reserve",
        "volume_mmbbl": 172.0,
        "returned_volume_mmbbl": 200.0,
        "delivery_start": "2026-03-20",
        "delivery_end": "",
        "pace_note": "Expected over about 120 days; exchange barrels to be returned within one year.",
        "sites": "Multiple SPR sites",
        "quality": "Crude oil; site/quality allocation not fully specified in announcement",
        "counterparties": "",
        "source_url": "https://www.energy.gov/articles/united-states-release-172-million-barrels-oil-strategic-petroleum-reserve",
        "award_pdf_url": "",
        "notes": "Official U.S. contribution to coordinated emergency release.",
    },
    {
        "date": "2026-03-19",
        "event_type": "International coordination",
        "status": "Confirmed",
        "source": "IEA",
        "title": "IEA confirms member country contributions to collective action to release oil stocks",
        "volume_mmbbl": 172.2,
        "returned_volume_mmbbl": "",
        "delivery_start": "",
        "delivery_end": "",
        "pace_note": "U.S. contribution listed as crude oil from public stocks.",
        "sites": "U.S. public stocks",
        "quality": "Crude oil",
        "counterparties": "",
        "source_url": "https://www.iea.org/news/iea-confirms-member-country-contributions-to-collective-action-to-release-oil-stocks-in-response-to-middle-east-disruptions",
        "award_pdf_url": "",
        "notes": "IEA public table lists 172.2 MMbbl U.S. contribution.",
    },
    {
        "date": "2026-03-20",
        "event_type": "Award",
        "status": "Awarded",
        "source": "DOE",
        "title": "Energy Department begins delivering SPR barrels at record speeds",
        "volume_mmbbl": 45.2,
        "returned_volume_mmbbl": "",
        "delivery_start": "2026-03-20",
        "delivery_end": "",
        "pace_note": "First tranche awarded and deliveries began.",
        "sites": "SPR sites; specific split not fully specified in summary page",
        "quality": "Crude oil",
        "counterparties": "",
        "source_url": "https://www.energy.gov/hgeo/articles/energy-department-begins-delivering-spr-barrels-record-speeds",
        "award_pdf_url": "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf",
        "notes": "First public execution tranche.",
    },
    {
        "date": "2026-04-10",
        "event_type": "Award",
        "status": "Awarded",
        "source": "DOE",
        "title": "Energy Department awards contracts for 8.5 million barrels in SPR second phase",
        "volume_mmbbl": 8.5,
        "returned_volume_mmbbl": "",
        "delivery_start": "",
        "delivery_end": "",
        "pace_note": "Second-phase emergency exchange award.",
        "sites": "Not fully specified in summary page",
        "quality": "Crude oil",
        "counterparties": "",
        "source_url": "https://www.energy.gov/hgeo/opr/articles/energy-department-awards-contracts-85-million-barrels-spr-second-phase-emergency",
        "award_pdf_url": "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.a/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.a%20Award%20Information.pdf",
        "notes": "Second public execution tranche.",
    },
    {
        "date": "2026-04-17",
        "event_type": "Award",
        "status": "Awarded",
        "source": "DOE",
        "title": "Energy Department awards new contracts for Strategic Petroleum Reserve",
        "volume_mmbbl": 26.0,
        "returned_volume_mmbbl": "",
        "delivery_start": "",
        "delivery_end": "",
        "pace_note": "Additional emergency exchange contracts awarded.",
        "sites": "Not fully specified in summary page",
        "quality": "Crude oil",
        "counterparties": "",
        "source_url": "https://www.energy.gov/hgeo/opr/articles/energy-department-awards-new-contracts-strategic-petroleum-reserve-advancing",
        "award_pdf_url": "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf",
        "notes": "Third public execution tranche.",
    },
    {
        "date": "2026-04-29",
        "event_type": "Inventory snapshot",
        "status": "Observed",
        "source": "DOE",
        "title": "SPR Quick Facts",
        "volume_mmbbl": 402.0,
        "returned_volume_mmbbl": "",
        "delivery_start": "",
        "delivery_end": "",
        "pace_note": "DOE site/quality inventory snapshot.",
        "sites": "Bayou Choctaw; Big Hill; Bryan Mound; West Hackberry",
        "quality": "150 MMbbl sweet; 252 MMbbl sour",
        "counterparties": "",
        "source_url": "https://www.energy.gov/hgeo/opr/spr-quick-facts",
        "award_pdf_url": "",
        "notes": "DOE quick facts inventory split by quality.",
    },
    {
        "date": "2026-04-30",
        "event_type": "RFP",
        "status": "Solicited",
        "source": "DOE",
        "title": "Energy Department issues RFP to continue execution of 172 million barrel release",
        "volume_mmbbl": 92.5,
        "returned_volume_mmbbl": "",
        "delivery_start": "",
        "delivery_end": "",
        "pace_note": "RFP for remaining emergency exchange volume; award pending in seeded baseline.",
        "sites": "Not fully specified in summary page",
        "quality": "Crude oil",
        "counterparties": "",
        "source_url": "https://www.energy.gov/hgeo/opr/articles/energy-department-issues-rfp-continue-swift-execution-president-trumps-172",
        "award_pdf_url": "",
        "notes": "RFP volume. A later DOE SPR award-information PDF shows 53.33 MMbbl awarded as of May 11, 2026.",
    },
    {
        "date": "2026-05-11",
        "event_type": "Award",
        "status": "Awarded",
        "source": "DOE SPR",
        "title": "FY26 SPR Oil Release No. 2 Award Information",
        "volume_mmbbl": 53.33,
        "returned_volume_mmbbl": "",
        "delivery_start": "",
        "delivery_end": "",
        "pace_note": "Award-information PDF shows 53.33 MMbbl awarded under RFP No. DE-RP96-26PO00004.",
        "sites": "Bayou Choctaw / Bryan Mound / Big Hill / West Hackberry",
        "quality": "Buyer-to-site and sweet/sour split not published in award PDF",
        "counterparties": "Atlantic; BP; Energy Transfer; ExxonMobil; Macquarie; Marathon; Mercuria; Phillips 66; Trafigura",
        "source_url": "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf",
        "award_pdf_url": "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf",
        "notes": "Current DOE SPR posting API exposes this award PDF.",
    },
]


SITE_QUALITY_ROWS = [
    {
        "as_of_date": "2026-04-29",
        "site": "Bayou Choctaw",
        "state": "Louisiana",
        "caverns": 6,
        "sweet_mmbbl": 0.0,
        "sour_mmbbl": 69.0,
        "total_mmbbl": 69.0,
        "source_url": "https://www.energy.gov/hgeo/opr/spr-quick-facts",
    },
    {
        "as_of_date": "2026-04-29",
        "site": "Big Hill",
        "state": "Texas",
        "caverns": 14,
        "sweet_mmbbl": 0.0,
        "sour_mmbbl": 162.0,
        "total_mmbbl": 162.0,
        "source_url": "https://www.energy.gov/hgeo/opr/spr-quick-facts",
    },
    {
        "as_of_date": "2026-04-29",
        "site": "Bryan Mound",
        "state": "Texas",
        "caverns": 18,
        "sweet_mmbbl": 130.0,
        "sour_mmbbl": 0.0,
        "total_mmbbl": 130.0,
        "source_url": "https://www.energy.gov/hgeo/opr/spr-quick-facts",
    },
    {
        "as_of_date": "2026-04-29",
        "site": "West Hackberry",
        "state": "Louisiana",
        "caverns": 21,
        "sweet_mmbbl": 20.0,
        "sour_mmbbl": 21.0,
        "total_mmbbl": 41.0,
        "source_url": "https://www.energy.gov/hgeo/opr/spr-quick-facts",
    },
]


RELEASE_QUALITY_ROWS = [
    {
        "date": "2026-03-20",
        "week": "2026-03-20",
        "tranche": "FY26 SPR Oil Release No. 1",
        "status": "Awarded",
        "site": "Bayou Choctaw",
        "volume_mmbbl": 10.0,
        "quality_bucket": "Sour",
        "quality_method": "Site quality classification",
        "source_url": "https://www.energy.gov/hgeo/articles/energy-department-begins-delivering-spr-barrels-record-speeds",
        "notes": "DOE states 10 MMbbl from Bayou Choctaw. DOE quick facts show Bayou Choctaw inventory as sour.",
    },
    {
        "date": "2026-03-20",
        "week": "2026-03-20",
        "tranche": "FY26 SPR Oil Release No. 1",
        "status": "Awarded",
        "site": "Bryan Mound",
        "volume_mmbbl": 15.7,
        "quality_bucket": "Sweet",
        "quality_method": "Site quality classification",
        "source_url": "https://www.energy.gov/hgeo/articles/energy-department-begins-delivering-spr-barrels-record-speeds",
        "notes": "DOE states 15.7 MMbbl from Bryan Mound. DOE quick facts show Bryan Mound inventory as sweet.",
    },
    {
        "date": "2026-03-20",
        "week": "2026-03-20",
        "tranche": "FY26 SPR Oil Release No. 1",
        "status": "Awarded",
        "site": "West Hackberry",
        "volume_mmbbl": 19.5,
        "quality_bucket": "Mixed / split not stated",
        "quality_method": "Public quality split not stated",
        "source_url": "https://www.energy.gov/hgeo/articles/energy-department-begins-delivering-spr-barrels-record-speeds",
        "notes": "DOE states 19.5 MMbbl from West Hackberry. DOE quick facts show West Hackberry stores both sweet and sour, but the award page does not split the release by quality.",
    },
    {
        "date": "2026-04-10",
        "week": "2026-04-10",
        "tranche": "FY26 SPR Oil Release No. 1.a",
        "status": "Awarded",
        "site": "Bryan Mound",
        "volume_mmbbl": 8.48,
        "quality_bucket": "Sweet",
        "quality_method": "Site quality classification",
        "source_url": "https://www.energy.gov/hgeo/opr/articles/energy-department-awards-contracts-85-million-barrels-spr-second-phase-emergency",
        "notes": "DOE award PDF totals 8.48 MMbbl. DOE article identifies Bryan Mound as the origin site; DOE quick facts show Bryan Mound inventory as sweet.",
    },
    {
        "date": "2026-04-17",
        "week": "2026-04-17",
        "tranche": "FY26 SPR Oil Release No. 1.b",
        "status": "Awarded",
        "site": "West Hackberry",
        "volume_mmbbl": 26.03,
        "quality_bucket": "Mixed / split not stated",
        "quality_method": "Public quality split not stated",
        "source_url": "https://www.energy.gov/hgeo/opr/articles/energy-department-awards-new-contracts-strategic-petroleum-reserve-advancing",
        "notes": "DOE award PDF totals 26.03 MMbbl. DOE article identifies West Hackberry as the origin site; DOE quick facts show West Hackberry stores both sweet and sour, but the public award page does not split the release by quality.",
    },
    {
        "date": "2026-05-11",
        "week": "2026-05-11",
        "tranche": "FY26 SPR Oil Release No. 2",
        "status": "Awarded",
        "site": "Bayou Choctaw / Bryan Mound / Big Hill / West Hackberry",
        "volume_mmbbl": 53.33,
        "quality_bucket": "Public split not stated",
        "quality_method": "Buyer award PDF does not allocate barrels by site or quality",
        "source_url": "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf",
        "notes": "DOE current award PDF totals 53.33 MMbbl. The public No. 2 RFP spans all four sites, but the award PDF does not publish buyer-to-site or sweet/sour allocation.",
    },
    {
        "date": "2026-05-11",
        "week": "2026-05-11",
        "tranche": "FY26 SPR Oil Release No. 2 remaining",
        "status": "Open / not awarded",
        "site": "Bayou Choctaw / Bryan Mound / Big Hill / West Hackberry",
        "volume_mmbbl": 39.17,
        "quality_bucket": "Open RFP balance",
        "quality_method": "RFP quality split not yet awarded",
        "source_url": "https://www.spr.doe.gov/posting/exchange/current",
        "notes": "RFP No. 2 solicited 92.5 MMbbl; 53.33 MMbbl was awarded as of May 11, 2026, leaving 39.17 MMbbl not awarded in the public award information.",
    },
]


RELEASE_BUYER_ROWS = [
    ["2026-03-20", "2026-03-20", "FY26 SPR Oil Release No. 1", "BP Products North America", 5.0, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf"],
    ["2026-03-20", "2026-03-20", "FY26 SPR Oil Release No. 1", "Energy Transfer Crude Marketing LLC", 0.375, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf"],
    ["2026-03-20", "2026-03-20", "FY26 SPR Oil Release No. 1", "Gunvor USA LLC", 3.085, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf"],
    ["2026-03-20", "2026-03-20", "FY26 SPR Oil Release No. 1", "Marathon Petroleum Company LP", 7.7, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf"],
    ["2026-03-20", "2026-03-20", "FY26 SPR Oil Release No. 1", "Mercuria Energy America LLC", 2.0, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf"],
    ["2026-03-20", "2026-03-20", "FY26 SPR Oil Release No. 1", "Shell Trading (US) Company", 16.2, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf"],
    ["2026-03-20", "2026-03-20", "FY26 SPR Oil Release No. 1", "Trafigura Trading LLC", 8.86, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf"],
    ["2026-03-20", "2026-03-20", "FY26 SPR Oil Release No. 1", "Vitol Inc.", 2.0, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201%20Award%20Information.pdf"],
    ["2026-04-10", "2026-04-10", "FY26 SPR Oil Release No. 1.a", "Gunvor USA LLC", 1.1, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.a/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.a%20Award%20Information.pdf"],
    ["2026-04-10", "2026-04-10", "FY26 SPR Oil Release No. 1.a", "Macquarie Commodities Trading US", 2.0, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.a/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.a%20Award%20Information.pdf"],
    ["2026-04-10", "2026-04-10", "FY26 SPR Oil Release No. 1.a", "Phillips 66 Company", 2.9, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.a/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.a%20Award%20Information.pdf"],
    ["2026-04-10", "2026-04-10", "FY26 SPR Oil Release No. 1.a", "Trafigura Trading LLC", 2.48, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.a/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.a%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "Alon USA", 1.0, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "BP Products North America", 1.0, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "Energy Transfer Crude Marketing", 1.1, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "ExxonMobil Oil Corporation", 3.0, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "Macquarie Commodities Trading US", 2.5, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "Marathon Petroleum Company", 2.0, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "Shell Trading (US) Company", 1.9, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "Trafigura Trading LLC", 10.03, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-04-17", "2026-04-17", "FY26 SPR Oil Release No. 1.b", "Vitol", 3.5, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/archive/FY26%20SPR%20Oil%20Release%20No.%201.b/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%201.b%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "Atlantic Trading & Marketing", 1.7, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "BP Products North America", 2.1, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "Energy Transfer Crude Marketing", 1.05, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "ExxonMobil Oil Corporation", 11.4, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "Macquarie Commodities Trading US", 6.55, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "Marathon Petroleum Company", 12.4, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "Mercuria Energy America", 2.5, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "Phillips 66", 2.65, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
    ["2026-05-11", "2026-05-11", "FY26 SPR Oil Release No. 2", "Trafigura Trading LLC", 12.98, "Awarded", "https://www.spr.doe.gov/posting/EXCHANGE/FY26%20SPR%20Oil%20Release%20No.%202/1-Request%20for%20Proposal/FY26%20SPR%20Oil%20Release%20No.%202%20Award%20Information.pdf"],
]


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _extract_page_metadata(url: str) -> dict[str, Any]:
    """Best-effort official-page metadata; seeded facts remain authoritative."""
    if BeautifulSoup is None:
        return {}
    try:
        response = requests.get(url, headers={"User-Agent": "MacroDataTracker/1.0"}, timeout=25)
        response.raise_for_status()
    except Exception:
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    title = ""
    for selector in ['meta[property="og:title"]', 'meta[name="twitter:title"]']:
        tag = soup.select_one(selector)
        if tag and tag.get("content"):
            title = _clean_text(tag["content"])
            break
    if not title and soup.title:
        title = _clean_text(soup.title.get_text(" "))

    description = ""
    for selector in ['meta[name="description"]', 'meta[property="og:description"]']:
        tag = soup.select_one(selector)
        if tag and tag.get("content"):
            description = _clean_text(tag["content"])
            break

    published_date = ""
    for selector in [
        'meta[property="article:published_time"]',
        'meta[name="date"]',
        'time[datetime]',
    ]:
        tag = soup.select_one(selector)
        if not tag:
            continue
        published_date = tag.get("content") or tag.get("datetime") or _clean_text(tag.get_text(" "))
        if published_date:
            published_date = published_date[:10]
            break

    return {
        "page_title": title,
        "page_description": description,
        "page_published_date": published_date,
        "last_checked": datetime.now().isoformat(timespec="seconds"),
    }


def build_release_events(enrich_pages: bool = True) -> pd.DataFrame:
    events = pd.DataFrame(OFFICIAL_SOURCE_EVENTS)
    if enrich_pages:
        meta_rows = []
        for url in events["source_url"].dropna().unique():
            meta = _extract_page_metadata(url)
            meta_rows.append({"source_url": url, **meta})
            time.sleep(0.2)
        meta_df = pd.DataFrame(meta_rows)
        if not meta_df.empty:
            events = events.merge(meta_df, on="source_url", how="left")
    events["date"] = pd.to_datetime(events["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    events["volume_mmbbl"] = pd.to_numeric(events["volume_mmbbl"], errors="coerce")
    return events


def build_site_quality() -> pd.DataFrame:
    df = pd.DataFrame(SITE_QUALITY_ROWS)
    numeric_cols = ["caverns", "sweet_mmbbl", "sour_mmbbl", "total_mmbbl"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def build_release_quality() -> pd.DataFrame:
    df = pd.DataFrame(RELEASE_QUALITY_ROWS)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["week"] = pd.to_datetime(df["week"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["volume_mmbbl"] = pd.to_numeric(df["volume_mmbbl"], errors="coerce")
    return df


def build_release_buyers() -> pd.DataFrame:
    df = pd.DataFrame(
        RELEASE_BUYER_ROWS,
        columns=["date", "week", "tranche", "buyer", "volume_mmbbl", "status", "source_url"],
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["week"] = pd.to_datetime(df["week"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["volume_mmbbl"] = pd.to_numeric(df["volume_mmbbl"], errors="coerce")
    df["buyer_site_allocation"] = "Not published in DOE award information"
    df["quality_allocation"] = "Use tranche/site quality table; buyer-level sweet/sour split is not published"
    return df


def fetch_eia_spr_weekly(start_date: str = "2026-01-01") -> pd.DataFrame:
    api_key = API_KEYS.get("EIA") or os.environ.get("EIA_API_KEY", "")
    if not api_key:
        raise RuntimeError("EIA_API_KEY is required for live SPR weekly inventory refresh.")

    rows: list[dict[str, Any]] = []
    offset = 0
    length = 5000
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
        response = requests.get(EIA_WSTK_URL, params=params, timeout=35)
        response.raise_for_status()
        payload = response.json()
        chunk = payload.get("response", {}).get("data", [])
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
    df = df[["date", "spr_stock_mmbbl", "series", "series-description", "units"]].copy()
    return add_inventory_metrics(df)


def add_inventory_metrics(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy().sort_values("date")
    pre_release = working[working["date"] < PLAN_START_DATE]
    baseline_stock = (
        float(pre_release.iloc[-1]["spr_stock_mmbbl"])
        if not pre_release.empty
        else float(working.iloc[0]["spr_stock_mmbbl"])
    )
    baseline_date = (
        pre_release.iloc[-1]["date"]
        if not pre_release.empty
        else working.iloc[0]["date"]
    )

    working["stock_change_mmbbl"] = working["spr_stock_mmbbl"].diff()
    working["weekly_drawdown_mmbbl"] = -working["stock_change_mmbbl"]
    working["baseline_date"] = pd.Timestamp(baseline_date).strftime("%Y-%m-%d")
    working["baseline_stock_mmbbl"] = baseline_stock
    working["observed_drawdown_mmbbl"] = baseline_stock - working["spr_stock_mmbbl"]
    working["days_since_plan_start"] = (working["date"] - PLAN_START_DATE).dt.days.clip(lower=0)
    working["planned_drawdown_mmbbl"] = (
        working["days_since_plan_start"] * PLANNED_DAILY_MMBL
    ).clip(upper=ANNOUNCED_VOLUME_MMBL)
    working["plan_gap_mmbbl"] = working["observed_drawdown_mmbbl"] - working["planned_drawdown_mmbbl"]
    working["planned_weekly_mmbbl"] = PLANNED_WEEKLY_MMBL
    working["date"] = working["date"].dt.strftime("%Y-%m-%d")
    return working


def build_release_summary(
    weekly: pd.DataFrame,
    events: pd.DataFrame,
    site_quality: pd.DataFrame,
    release_quality: pd.DataFrame | None = None,
) -> pd.DataFrame:
    latest = weekly.sort_values("date").iloc[-1] if not weekly.empty else {}
    observed_drawdown = float(latest.get("observed_drawdown_mmbbl", 0) or 0)
    planned_drawdown = float(latest.get("planned_drawdown_mmbbl", 0) or 0)
    latest_weekly_pace = float(latest.get("weekly_drawdown_mmbbl", 0) or 0)
    latest_stock = float(latest.get("spr_stock_mmbbl", 0) or 0)
    latest_date = latest.get("date", "")
    release_quality = release_quality if release_quality is not None else build_release_quality()
    awarded_quality = release_quality[release_quality["status"].eq("Awarded")]
    open_quality = release_quality[release_quality["status"].eq("Open / not awarded")]
    quality_totals = awarded_quality.groupby("quality_bucket")["volume_mmbbl"].sum().to_dict()
    award_volume = awarded_quality["volume_mmbbl"].sum()
    open_volume = open_quality["volume_mmbbl"].sum()

    rows = [
        {"metric": "announced_release_mmbbl", "value": ANNOUNCED_VOLUME_MMBL, "unit": "MMbbl", "label": "Announced release"},
        {"metric": "iea_us_contribution_mmbbl", "value": IEA_US_CONTRIBUTION_MMBL, "unit": "MMbbl", "label": "IEA U.S. contribution"},
        {"metric": "expected_return_mmbbl", "value": RETURN_EXPECTED_MMBL, "unit": "MMbbl", "label": "Expected returned volume"},
        {"metric": "release_days", "value": PLAN_DAYS, "unit": "days", "label": "Announced release window"},
        {"metric": "planned_daily_mmbbl", "value": PLANNED_DAILY_MMBL, "unit": "MMbbl/day", "label": "Implied planned daily pace"},
        {"metric": "planned_weekly_mmbbl", "value": PLANNED_WEEKLY_MMBL, "unit": "MMbbl/week", "label": "Implied planned weekly pace"},
        {"metric": "awarded_mmbbl", "value": float(award_volume), "unit": "MMbbl", "label": "Awarded volume"},
        {"metric": "solicited_not_awarded_mmbbl", "value": float(open_volume), "unit": "MMbbl", "label": "Solicited-not-awarded volume"},
        {"metric": "observed_eia_drawdown_mmbbl", "value": observed_drawdown, "unit": "MMbbl", "label": "Observed EIA drawdown"},
        {"metric": "planned_drawdown_to_latest_mmbbl", "value": planned_drawdown, "unit": "MMbbl", "label": "Planned drawdown to latest EIA week"},
        {"metric": "plan_gap_mmbbl", "value": observed_drawdown - planned_drawdown, "unit": "MMbbl", "label": "Actual minus planned drawdown"},
        {"metric": "latest_spr_stock_mmbbl", "value": latest_stock, "unit": "MMbbl", "label": "Latest SPR stock"},
        {"metric": "latest_weekly_drawdown_mmbbl", "value": latest_weekly_pace, "unit": "MMbbl/week", "label": "Latest weekly drawdown pace"},
        {"metric": "released_sweet_mmbbl", "value": float(quality_totals.get("Sweet", 0)), "unit": "MMbbl", "label": "Awarded release classified sweet"},
        {"metric": "released_sour_mmbbl", "value": float(quality_totals.get("Sour", 0)), "unit": "MMbbl", "label": "Awarded release classified sour"},
        {"metric": "released_mixed_split_not_stated_mmbbl", "value": float(quality_totals.get("Mixed / split not stated", 0)), "unit": "MMbbl", "label": "Awarded release mixed / split not stated"},
        {"metric": "released_public_split_not_stated_mmbbl", "value": float(quality_totals.get("Public split not stated", 0)), "unit": "MMbbl", "label": "Awarded release public split not stated"},
        {"metric": "solicited_quality_unspecified_mmbbl", "value": float(open_volume), "unit": "MMbbl", "label": "Open RFP balance quality unspecified"},
        {"metric": "site_total_mmbbl", "value": float(site_quality["total_mmbbl"].sum()), "unit": "MMbbl", "label": "DOE quick-facts site total"},
        {"metric": "site_sweet_mmbbl", "value": float(site_quality["sweet_mmbbl"].sum()), "unit": "MMbbl", "label": "DOE sweet crude inventory"},
        {"metric": "site_sour_mmbbl", "value": float(site_quality["sour_mmbbl"].sum()), "unit": "MMbbl", "label": "DOE sour crude inventory"},
    ]
    summary = pd.DataFrame(rows)
    summary["as_of_date"] = latest_date
    summary["refreshed_at"] = datetime.now().isoformat(timespec="seconds")
    return summary


def build_news(events: pd.DataFrame) -> pd.DataFrame:
    cols = [
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
        "page_title",
        "page_description",
        "last_checked",
    ]
    available = [col for col in cols if col in events.columns]
    news = events[available].copy().sort_values("date", ascending=False)
    return news


def fetch_all(enrich_pages: bool = True) -> dict[str, int]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 50)
    print("SPR Release Fetcher")
    print(f"Timestamp: {datetime.now().isoformat(timespec='seconds')}")
    print("=" * 50)

    events = build_release_events(enrich_pages=enrich_pages)
    site_quality = build_site_quality()
    release_quality = build_release_quality()
    release_buyers = build_release_buyers()
    weekly = fetch_eia_spr_weekly()
    summary = build_release_summary(weekly, events, site_quality, release_quality)
    news = build_news(events)

    outputs = {
        "spr_release_events.csv": events,
        "spr_site_quality.csv": site_quality,
        "spr_release_quality.csv": release_quality,
        "spr_release_buyers.csv": release_buyers,
        "spr_weekly_inventory.csv": weekly,
        "spr_release_summary.csv": summary,
        "spr_news.csv": news,
    }
    for filename, df in outputs.items():
        df.to_csv(DATA_DIR / filename, index=False)
        print(f"  Saved {len(df)} rows to {filename}")

    print("=" * 50)
    print("Done!")
    print("=" * 50)
    return {filename: len(df) for filename, df in outputs.items()}


if __name__ == "__main__":
    try:
        fetch_all(enrich_pages=True)
    except Exception as exc:
        print(f"SPR refresh failed: {exc}", file=sys.stderr)
        raise
