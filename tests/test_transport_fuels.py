"""Guard the imported snapshot's coverage and unit arithmetic."""
import json
from pathlib import Path
import unittest


class TransportFuelSnapshotTests(unittest.TestCase):
    def test_coverage_and_contributions(self):
        countries = json.loads((Path(__file__).resolve().parents[1] / 'static/transport-fuels/data.json').read_text())
        self.assertEqual(len(countries), 10)
        self.assertGreaterEqual(sum(len(c['rows']) for c in countries), 605)
        for country in countries:
            rows = {r['date']: r for r in country['rows']}
            self.assertEqual(len(rows), len(country['rows']))
            self.assertEqual(min(rows), '2021-07-01')
            for date, row in rows.items():
                with self.subTest(country=country['country'], date=date):
                    self.assertAlmostEqual(sum(row[p + '_yoy_mbd'] for p in ('gasoline', 'jet_fuel', 'diesel')), row['net_yoy_mbd'], places=7)
                    prior = rows.get(str(int(date[:4]) - 1) + date[4:])
                    if prior:
                        for product in ('gasoline', 'jet_fuel', 'diesel'):
                            self.assertAlmostEqual(row[product + '_mbd'] - prior[product + '_mbd'], row[product + '_yoy_mbd'], places=7)


if __name__ == '__main__':
    unittest.main()
