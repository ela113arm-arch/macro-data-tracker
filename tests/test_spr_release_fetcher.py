import unittest

import pandas as pd

from spr_release_fetcher import (
    ANNOUNCED_VOLUME_MMBL,
    PLANNED_DAILY_MMBL,
    PLANNED_WEEKLY_MMBL,
    add_inventory_metrics,
    build_release_events,
    build_release_buyers,
    build_release_quality,
    build_release_summary,
    build_site_quality,
    parse_eia_spr_rows,
)


class SprReleaseFetcherTests(unittest.TestCase):
    def test_seeded_events_capture_public_release_facts(self):
        events = build_release_events(enrich_pages=False)

        awarded = events.loc[events["status"].eq("Awarded"), "volume_mmbbl"].sum()
        solicited = events.loc[events["status"].eq("Solicited"), "volume_mmbbl"].sum()
        announced = events.loc[events["event_type"].eq("Announcement"), "volume_mmbbl"].iloc[0]

        self.assertEqual(announced, ANNOUNCED_VOLUME_MMBL)
        self.assertAlmostEqual(awarded, 133.03, places=2)
        self.assertAlmostEqual(solicited, 92.5, places=1)
        self.assertTrue(events["source_url"].str.startswith("https://").all())

    def test_eia_rows_are_converted_to_mmbbl_and_drawdown(self):
        rows = [
            {"period": "2026-03-13", "value": "420000", "series": "WCSSTUS1", "series-description": "SPR", "units": "MBBL"},
            {"period": "2026-03-20", "value": "418000", "series": "WCSSTUS1", "series-description": "SPR", "units": "MBBL"},
            {"period": "2026-03-27", "value": "410000", "series": "WCSSTUS1", "series-description": "SPR", "units": "MBBL"},
        ]

        weekly = parse_eia_spr_rows(rows)

        self.assertEqual(weekly.iloc[0]["spr_stock_mmbbl"], 420.0)
        self.assertEqual(weekly.iloc[-1]["baseline_stock_mmbbl"], 420.0)
        self.assertEqual(weekly.iloc[-1]["observed_drawdown_mmbbl"], 10.0)
        self.assertEqual(weekly.iloc[-1]["weekly_drawdown_mmbbl"], 8.0)
        self.assertAlmostEqual(weekly.iloc[-1]["planned_weekly_mmbbl"], PLANNED_WEEKLY_MMBL, places=3)

    def test_summary_metrics_match_seeded_public_facts(self):
        weekly = add_inventory_metrics(pd.DataFrame({
            "date": pd.to_datetime(["2026-03-13", "2026-05-01"]),
            "spr_stock_mmbbl": [420.0, 392.7],
            "series": ["WCSSTUS1", "WCSSTUS1"],
            "series-description": ["SPR", "SPR"],
            "units": ["MBBL", "MBBL"],
        }))
        events = build_release_events(enrich_pages=False)
        site_quality = build_site_quality()
        release_quality = build_release_quality()

        summary = build_release_summary(weekly, events, site_quality, release_quality)
        values = dict(zip(summary["metric"], summary["value"]))

        self.assertAlmostEqual(values["planned_daily_mmbbl"], PLANNED_DAILY_MMBL, places=6)
        self.assertAlmostEqual(values["awarded_mmbbl"], 133.04, places=2)
        self.assertAlmostEqual(values["solicited_not_awarded_mmbbl"], 39.17, places=2)
        self.assertAlmostEqual(values["observed_eia_drawdown_mmbbl"], 27.3, places=1)
        self.assertAlmostEqual(values["released_sweet_mmbbl"], 24.18, places=2)
        self.assertAlmostEqual(values["released_sour_mmbbl"], 10.0, places=1)
        self.assertAlmostEqual(values["released_mixed_split_not_stated_mmbbl"], 45.53, places=2)
        self.assertAlmostEqual(values["released_public_split_not_stated_mmbbl"], 53.33, places=2)
        self.assertAlmostEqual(values["solicited_quality_unspecified_mmbbl"], 39.17, places=2)
        self.assertAlmostEqual(values["site_sweet_mmbbl"], 150.0, places=1)
        self.assertAlmostEqual(values["site_sour_mmbbl"], 252.0, places=1)

    def test_release_quality_tracks_public_site_classification(self):
        release_quality = build_release_quality()
        awarded = release_quality[release_quality["status"].eq("Awarded")]
        by_quality = awarded.groupby("quality_bucket")["volume_mmbbl"].sum().to_dict()

        self.assertAlmostEqual(by_quality["Sweet"], 24.18, places=2)
        self.assertAlmostEqual(by_quality["Sour"], 10.0, places=1)
        self.assertAlmostEqual(by_quality["Mixed / split not stated"], 45.53, places=2)
        self.assertAlmostEqual(by_quality["Public split not stated"], 53.33, places=2)
        self.assertAlmostEqual(
            release_quality.loc[release_quality["status"].eq("Open / not awarded"), "volume_mmbbl"].sum(),
            39.17,
            places=2,
        )
        self.assertIn("not stated", release_quality.loc[release_quality["site"].eq("West Hackberry"), "quality_method"].iloc[0])

    def test_release_buyers_match_public_award_totals(self):
        buyers = build_release_buyers()
        by_tranche = buyers.groupby("tranche")["volume_mmbbl"].sum().to_dict()

        self.assertAlmostEqual(by_tranche["FY26 SPR Oil Release No. 1"], 45.22, places=2)
        self.assertAlmostEqual(by_tranche["FY26 SPR Oil Release No. 1.a"], 8.48, places=2)
        self.assertAlmostEqual(by_tranche["FY26 SPR Oil Release No. 1.b"], 26.03, places=2)
        self.assertAlmostEqual(by_tranche["FY26 SPR Oil Release No. 2"], 53.33, places=2)
        self.assertIn("buyer-level", buyers["quality_allocation"].iloc[0])


if __name__ == "__main__":
    unittest.main()
