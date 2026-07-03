import unittest

import pandas as pd

from spr_release_fetcher import (
    ANNOUNCED_VOLUME_MMBL,
    PLANNED_DAILY_MMBL,
    PLANNED_WEEKLY_MMBL,
    add_inventory_metrics,
    build_monthly_summary,
    build_release_quality,
    build_release_summary,
    build_site_quality,
    parse_award_pdf,
    parse_delivery_rates,
    parse_eia_spr_rows,
    parse_rfp_plan,
)


RFP_NO_3_TEXT = """
SECTION C
C.1 SCOPE OF WORK
The Contractor shall accept up to 40,000,000 barrels of SPR Exchange Oil.
Bryan Mound Sour 8,000,000 Jul-26 Bryan Mound 8,000,000* 0.0%
Bryan Mound Sour 7,000,000 Sep-26 Bryan Mound 7,000,000* 0.0%
Big Hill Sour 11,000,000 Aug-26 Big Hill 11,000,000* 0.0%
Big Hill Sour 14,000,000 Sep-26 Big Hill 14,000,000* 0.0%
SECTION D
"""

RFP_NO_1_TEXT = """
SECTION C
C.1 SCOPE OF WORK
The Contractor shall accept up to 86,000,000 barrels of SPR Exchange Oil.
Bryan Mound Sour 42,000,000 21,000,000 April - May 2026 Bryan Mound 42,000,000*
Bayou Choctaw Sweet 10,000,000 5,000,000 April - May 2026 Bayou Choctaw 10,000,000*
West Hackberry Sour 34,000,000 17,000,000 April - May 2026 West Hackberry 34,000,000*
SECTION D
"""

AWARD_TEXT = """
FY26 SPR Oil Release No. 3
EXCHANGE OF UP TO 40 MILLION BARRELS
As of June 22, 2026, the Department of Energy accepted bids for the exchange of 500,000 barrels.
Vitol Inc. - 500,000 barrels
"""

RATE_TABLE_TEXT = """
B.6 DELIVERY
d. Maximum daily capability to Contractor of Exchange Oil from the SPR sites can be found below:
Crude Oil Stream Mode of Delivery Barrels per day Limit
Freeport Docks 480,000
Pipeline (Texas C ity) 880,000
Big Hill Sour Nederland Pipeline 760,000
P66 Beaumont 480,000
Bryan Mound Sour
REQUEST FOR PROPOSAL
DE-RP96-26PO00005
Section B, Page B-9
Bryan Mound 225,000 barrels per day
B.7 CLOSE-OUT
"""


class SprReleaseFetcherTests(unittest.TestCase):
    def test_parse_rfp_section_c_new_month_format(self):
        plan, planned_total = parse_rfp_plan(
            "FY26 SPR Oil Release No. 3",
            "https://www.spr.doe.gov/posting/Exchange/FY26SPR3/rfp.pdf",
            RFP_NO_3_TEXT,
        )

        self.assertAlmostEqual(planned_total, 40.0)
        self.assertEqual(len(plan), 4)
        self.assertAlmostEqual(plan["volume_mmbbl"].sum(), 40.0)
        self.assertEqual(plan["quality_bucket"].unique().tolist(), ["Sour"])
        self.assertEqual(set(plan["site"]), {"Bryan Mound", "Big Hill"})
        self.assertEqual(plan.loc[plan["delivery_period"].eq("Jul-26"), "delivery_month"].iloc[0], "2026-07")
        self.assertGreater(plan["planned_avg_bpd"].min(), 0)

    def test_parse_rfp_section_c_legacy_range_format(self):
        plan, planned_total = parse_rfp_plan(
            "FY26 SPR Oil Release No. 1",
            "https://www.spr.doe.gov/posting/Exchange/archive/FY26SPR1/rfp.pdf",
            RFP_NO_1_TEXT,
        )

        self.assertAlmostEqual(planned_total, 86.0)
        self.assertEqual(len(plan), 3)
        self.assertAlmostEqual(plan["volume_mmbbl"].sum(), 86.0)
        self.assertEqual(set(plan["quality_bucket"]), {"Sweet", "Sour"})
        self.assertEqual(plan.loc[plan["site"].eq("Bayou Choctaw"), "delivery_month"].iloc[0], "2026-04")
        self.assertAlmostEqual(
            plan.loc[plan["site"].eq("Bryan Mound"), "nominal_delivery_rate_mmbbl_per_month"].iloc[0],
            21.0,
        )

    def test_parse_award_pdf_buyer_rows(self):
        buyers, summary = parse_award_pdf(
            "FY26 SPR Oil Release No. 3",
            "https://www.spr.doe.gov/posting/Exchange/FY26SPR3/award.pdf",
            AWARD_TEXT,
        )

        self.assertEqual(summary["date"], "2026-06-22")
        self.assertAlmostEqual(summary["award_total_mmbbl"], 0.5)
        self.assertAlmostEqual(summary["award_pdf_announced_up_to_mmbbl"], 40.0)
        self.assertEqual(summary["buyer_count"], 1)
        self.assertEqual(buyers.iloc[0]["buyer"], "Vitol Inc.")
        self.assertAlmostEqual(buyers.iloc[0]["volume_mmbbl"], 0.5)

    def test_parse_delivery_rates_handles_reordered_pdf_table_cells(self):
        rates = parse_delivery_rates(
            "FY26 SPR Oil Release No. 3",
            "https://www.spr.doe.gov/posting/Exchange/FY26SPR3/rfp.pdf",
            RATE_TABLE_TEXT,
        )
        by_mode = rates.set_index("mode_of_delivery")

        self.assertEqual(len(rates), 4)
        self.assertEqual(by_mode.loc["Freeport Docks", "site"], "Bryan Mound")
        self.assertEqual(by_mode.loc["Pipeline (Texas City)", "site"], "Bryan Mound")
        self.assertEqual(by_mode.loc["Nederland Pipeline", "site"], "Big Hill")
        self.assertEqual(by_mode.loc["P66 Beaumont", "site"], "Big Hill")
        self.assertEqual(by_mode.loc["Pipeline (Texas City)", "barrels_per_day_limit"], 880000.0)

    def test_eia_rows_are_as_of_friday_with_wednesday_publish_estimate(self):
        rows = [
            {"period": "2026-03-13", "value": "420000", "series": "WCSSTUS1", "series-description": "SPR", "units": "MBBL"},
            {"period": "2026-03-20", "value": "418000", "series": "WCSSTUS1", "series-description": "SPR", "units": "MBBL"},
            {"period": "2026-03-27", "value": "410000", "series": "WCSSTUS1", "series-description": "SPR", "units": "MBBL"},
        ]

        weekly = parse_eia_spr_rows(rows)

        self.assertEqual(weekly.iloc[0]["spr_stock_mmbbl"], 420.0)
        self.assertEqual(weekly.iloc[-1]["baseline_stock_mmbbl"], 420.0)
        self.assertEqual(weekly.iloc[-1]["eia_week_ending_friday"], "2026-03-27")
        self.assertEqual(weekly.iloc[-1]["eia_publish_date_estimate"], "2026-04-01")
        self.assertEqual(weekly.iloc[-1]["observed_drawdown_mmbbl"], 10.0)
        self.assertEqual(weekly.iloc[-1]["weekly_drawdown_mmbbl"], 8.0)
        self.assertAlmostEqual(weekly.iloc[-1]["planned_weekly_mmbbl"], PLANNED_WEEKLY_MMBL, places=3)

    def test_monthly_and_topline_summaries_use_live_source_shapes(self):
        plan, _ = parse_rfp_plan("FY26 SPR Oil Release No. 3", "https://example.com/rfp.pdf", RFP_NO_3_TEXT)
        buyers, award_summary_dict = parse_award_pdf("FY26 SPR Oil Release No. 3", "https://example.com/award.pdf", AWARD_TEXT)
        buyers["month"] = "2026-06"
        buyers["month_label"] = "Jun 2026"
        award_summary = pd.DataFrame([award_summary_dict])
        award_summary["month"] = "2026-06"
        award_summary["month_label"] = "Jun 2026"
        weekly = add_inventory_metrics(
            pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-03-13", "2026-06-19", "2026-06-26"]),
                    "spr_stock_mmbbl": [420.0, 390.0, 388.0],
                    "series": ["WCSSTUS1"] * 3,
                    "series-description": ["SPR"] * 3,
                    "units": ["MBBL"] * 3,
                }
            )
        )
        quality = build_release_quality(plan, award_summary)
        site_quality = build_site_quality(plan)

        monthly = build_monthly_summary(weekly, quality, buyers, plan, award_summary)
        summary = build_release_summary(weekly, pd.DataFrame(), site_quality, quality, plan, award_summary)
        values = dict(zip(summary["metric"], summary["value"]))

        self.assertAlmostEqual(values["announced_release_mmbbl"], ANNOUNCED_VOLUME_MMBL)
        self.assertAlmostEqual(values["planned_daily_mmbbl"], PLANNED_DAILY_MMBL)
        self.assertAlmostEqual(values["awarded_mmbbl"], 0.5)
        self.assertAlmostEqual(values["released_sour_mmbbl"], 0.5)
        self.assertAlmostEqual(values["planned_sour_rows_mmbbl"], 40.0)
        self.assertAlmostEqual(values["observed_eia_drawdown_mmbbl"], 32.0)
        self.assertIn("2026-06", set(monthly["month"]))
        self.assertAlmostEqual(
            monthly.loc[monthly["month"].eq("2026-06"), "awarded_release_mmbbl"].iloc[0],
            0.5,
        )


if __name__ == "__main__":
    unittest.main()
