import csv
from datetime import date, datetime, timezone
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('fuel_refresh', ROOT / 'scripts/update_transport_fuels.py')
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)


class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def extract(self, dates):
        path = self.root / 'source.csv'
        with path.open('w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['date', 'gasoline', 'jet_fuel', 'diesel', 'native_unit'])
            for d in dates:
                w.writerow([d, 1000, 200, 800, 'm3'])
        return path

    def test_calendar_days_and_prior_year(self):
        periods = []
        d = '2020-07-01'
        while d <= '2024-02-01':
            periods.append(d)
            d = refresh.next_month(d)
        rows = refresh.validated_rows(self.extract(periods), 'm3', date(2026, 9, 12))
        last = rows[-1]
        expected = 1000 * 6.28981077 / 1_000_000 * (1/29 - 1/28)
        self.assertAlmostEqual(last['gasoline_yoy_mbd'], expected)
        self.assertEqual(rows[0]['date'], '2021-07-01')
        self.assertAlmostEqual(refresh.convert(9000, 'thousand b/d', 'gasoline', '2026-02-01'), 9)

    def test_missing_month_rejected(self):
        with self.assertRaisesRegex(ValueError, 'gap'):
            refresh.validated_rows(self.extract(['2020-07-01', '2020-09-01']), 'm3', date(2026, 9, 12))

    def test_valid_country_advances_while_other_sources_fail(self):
        target = self.root / 'static/transport-fuels'
        shutil.copytree(ROOT / 'static/transport-fuels', target)
        original = json.loads((target / 'data.json').read_text())
        us = next(c for c in original if c['country'] == 'United States')
        # Synthetic test-only source history. Never used as production inputs.
        source = self.root / 'us_fixture.csv'
        with source.open('w', newline='') as handle:
            writer = csv.writer(handle)
            writer.writerow(['date', 'gasoline', 'jet_fuel', 'diesel', 'native_unit'])
            for row in us['rows'][:12]:
                writer.writerow([str(int(row['date'][:4])-1)+row['date'][4:]] +
                                [(row[p+'_mbd']-row[p+'_yoy_mbd'])*1000 for p in refresh.PRODUCTS] + ['thousand b/d'])
            for row in us['rows']:
                writer.writerow([row['date']] + [row[p+'_mbd']*1000 for p in refresh.PRODUCTS] + ['thousand b/d'])
            newest = refresh.next_month(us['rows'][-1]['date'])
            writer.writerow([newest, 8900, 1800, 3800, 'thousand b/d'])
        manifest = {c['country']: {'status':'error', 'message':'Fixture source failure'} for c in original}
        manifest['United States'] = {'status':'ok', 'csv':source.name,
            'source_urls':['https://www.eia.gov/dnav/pet/example.xls'],
            'vintage':'Synthetic fixture', 'validation_note':'Test only'}
        path = self.root / 'manifest.json'
        path.write_text(json.dumps(manifest))
        future = datetime.fromisoformat(refresh.next_month(newest)).replace(tzinfo=timezone.utc)
        failures = refresh.apply_manifest(path, self.root, future)
        self.assertEqual(len(failures), 9)
        updated = json.loads((target / 'data.json').read_text())
        latest = next(c for c in updated if c['country']=='United States')['rows'][-1]
        self.assertEqual(latest['date'], newest)
        self.assertEqual(latest['gasoline_mbd'], 8.9)
        for old in original:
            if old['country'] != 'United States':
                self.assertEqual(next(c for c in updated if c['country']==old['country']), old)
        with (target / 'standardized_transport_fuel_yoy_mbd_data.csv').open() as handle:
            rows = list(csv.DictReader(handle))
        self.assertTrue(any(r['country']=='United States' and r['date']==newest for r in rows))

    def test_failure_retains_data_and_last_success(self):
        target = self.root / 'static/transport-fuels'
        shutil.copytree(ROOT / 'static/transport-fuels', target)
        original = json.loads((target / 'data.json').read_text())
        status = json.loads((target / 'refresh-status.json').read_text())
        status['countries']['United States']['last_success_at'] = '2026-09-01T12:00:00+00:00'
        (target / 'refresh-status.json').write_text(json.dumps(status))
        manifest = {c['country']: {'status': 'error', 'message': 'Fixture: source unavailable'} for c in original}
        path = self.root / 'manifest.json'
        path.write_text(json.dumps(manifest))
        failures = refresh.apply_manifest(path, self.root, datetime(2026, 9, 12, tzinfo=timezone.utc))
        self.assertEqual(len(failures), 10)
        self.assertEqual(json.loads((target / 'data.json').read_text()), original)
        state = json.loads((target / 'refresh-status.json').read_text())['countries']['United States']
        self.assertEqual(state['last_success_at'], '2026-09-01T12:00:00+00:00')
        self.assertEqual(state['status'], 'error')


if __name__ == '__main__':
    unittest.main()
