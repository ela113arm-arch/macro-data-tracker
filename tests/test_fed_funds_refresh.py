"""Exercise the Treasury refresh without importing unrelated fetchers or credentials."""
import ast
from pathlib import Path
import tempfile
import unittest
import pandas as pd


class FedFundsRefreshTests(unittest.TestCase):
    def refresh(self, directory, observations):
        source = ast.parse(Path('data_fetcher.py').read_text())
        function = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == 'fetch_treasury_yields')
        calls = []
        def fetch(series_id, start):
            calls.append(series_id)
            return observations if series_id == 'DFF' else [('2026-09-18', 4.5)]
        namespace = {'pd': pd, 'DATA_DIR': directory, 'fetch_fred_series': fetch}
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'data_fetcher.py', 'exec'), namespace)
        return namespace['fetch_treasury_yields'], calls

    def test_daily_rates_preserve_dates_and_do_not_fill_newer_yields(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            refresh, calls = self.refresh(directory, [('2026-09-16', 3.63), ('2026-09-17', 3.88)])
            frame = refresh().set_index('date')
            self.assertIn('DFF', calls)
            self.assertNotIn('FEDFUNDS', calls)
            self.assertEqual(frame.loc['2026-09-17', 'fed_funds'], 3.88)
            self.assertTrue(pd.isna(frame.loc['2026-09-18', 'fed_funds']))

    def test_failed_daily_fetch_preserves_existing_file(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            file = directory / 'treasury_yields.csv'
            file.write_text('existing data')
            refresh, _ = self.refresh(directory, [])
            with self.assertRaises(ValueError):
                refresh()
            self.assertEqual(file.read_text(), 'existing data')

if __name__ == '__main__':
    unittest.main()
