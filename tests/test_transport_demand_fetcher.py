from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from scripts import transport_demand_fetcher as etl


class TransportDemandTests(unittest.TestCase):
    def test_tsa_daily_rows_exclude_monthly_summary_and_prior_year_column(self):
        html = '<table><tr><td>October 2026</td><td>70000000</td></tr>'
        html += ''.join(f'<tr><td>November {d}, 2026</td><td>2,000,{d:03d}</td><td>1,000,000</td></tr>' for d in range(1, 15))
        rows = etl.parse_tsa(html + '</table>')
        self.assertEqual(len(rows), 14)
        self.assertEqual(rows[-1], {'date': '2026-11-14', 'value': 2000014})

    def test_eurocontrol_selects_network_and_keeps_matched_reference_dates(self):
        payload = [['Entity', 'Day', 'Flights', 'Day Previous Year', 'Flights 2025 (Reference)'],
                   ['string', 'date', 'number', 'date', 'number']]
        for d in range(1, 15):
            payload += [['France', f'2026-11-{d:02d}', '9000', f'2025-11-{d:02d}', '8000'],
                        ['-Total Network Manager Area', f'2026-11-{d:02d}', '27000', f'2025-11-{d + 1:02d}', '26000']]
        rows = etl.parse_eurocontrol(payload)
        self.assertEqual(len(rows), 14)
        self.assertEqual(rows[-1]['reference_date'], '2025-11-15')
        self.assertEqual(rows[-1]['value'], 27000)
        self.assertEqual(rows[-1]['reference_value'], 26000)

    def test_china_units_and_cross_year_week(self):
        html = '''<meta name="PubDate" content="2026-01-05 15:49"><h1>12月29日—1月4日全国物流保通保畅运行情况</h1>
        <p>全国高速公路货车通行5,399.3万辆，环比下降3.13%</p><p>民航保障航班11.4万班</p>'''
        rows = etl.parse_china(html, 'https://www.mot.gov.cn/report.html')
        self.assertEqual(rows['china_trucks'][0]['value'], 53993000)
        self.assertEqual(rows['china_trucks'][0]['period_start'], '2025-12-29')
        self.assertEqual(rows['china_flights'][0]['value'], 114000)
        self.assertEqual(rows['china_flights'][0]['date'], '2026-01-04')

    def test_daily_and_non_seven_day_china_reports_are_rejected(self):
        for period in ('11月2日', '11月2日—11月4日'):
            html = f'<meta name="PubDate" content="2026-11-09"><h1>{period}全国物流保通保畅运行情况</h1>'
            with self.assertRaises(ValueError):
                etl.parse_china(html, 'https://www.mot.gov.cn/a.html')

    def test_china_archive_accepts_explicit_years_and_traffic_volume_wording(self):
        html = '''<meta name="PubDate" content="2025-01-06"><h1>2024年12月30日-2025年1月5日全国物流保通保畅运行情况</h1>
        <p>全国高速公路累计货车通行5300万辆</p><p>民航累计保障航班11万班</p>'''
        rows = etl.parse_china(html, 'https://www.mot.gov.cn/archive.html')
        self.assertEqual(rows['china_trucks'][0]['date'], '2025-01-05')
        self.assertEqual(rows['china_trucks'][0]['period_start'], '2024-12-30')
        self.assertEqual(rows['china_trucks'][0]['value'], 53000000)

    def test_invalid_future_zero_and_nan_values_are_rejected(self):
        for row in ({'date': '2026-11-13', 'value': 2000000},
                    {'date': '2026-11-12', 'value': 0},
                    {'date': '2026-11-12', 'value': float('nan')}):
            with self.assertRaises(ValueError):
                etl.validated_rows([row], date(2026, 11, 13), 'tsa_passengers')

    def test_new_window_preserves_history_and_revisions_replace_dates(self):
        old = [{'date': '2026-11-10', 'value': 1}, {'date': '2026-11-11', 'value': 2}]
        new = [{'date': '2026-11-11', 'value': 3}, {'date': '2026-11-12', 'value': 4}]
        merged = etl.merge_rows(old, new)
        self.assertEqual([r['value'] for r in merged], [1, 3, 4])

    def test_failed_or_stale_fetch_retains_last_validated_history(self):
        now = datetime(2026, 11, 13, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp)
            def good():
                return {'series': {'tsa_passengers': [{'date': '2026-11-12', 'value': 2000000}]}, 'source_url': etl.TSA_URL}
            self.assertEqual(etl.refresh(out, now, {'tsa': good}), [])
            before = json.loads((out / 'data.json').read_text())
            def bad():
                raise ValueError('source unavailable')
            self.assertEqual(etl.refresh(out, now, {'tsa': bad}), ['tsa'])
            after = json.loads((out / 'data.json').read_text())
            self.assertEqual(before['series'], after['series'])
            self.assertEqual(before['feeds']['tsa']['last_success_at'], after['feeds']['tsa']['last_success_at'])
            self.assertEqual(after['feeds']['tsa']['status'], 'error')
            def stale():
                return {'series': {'tsa_passengers': [{'date': '2026-11-01', 'value': 3000000}]}, 'source_url': etl.TSA_URL}
            self.assertEqual(etl.refresh(out, now, {'tsa': stale}), ['tsa'])
            self.assertEqual(json.loads((out / 'data.json').read_text())['series'], before['series'])


if __name__ == '__main__':
    unittest.main()
