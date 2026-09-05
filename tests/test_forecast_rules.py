import os
os.environ['MODELING_WARM_ON_BOOT'] = '0'
import unittest
import numpy as np
import pandas as pd
from forecast_rules import available_dates, choose_model
from app import add_asof_feature_block


class ForecastRulesTest(unittest.TestCase):
    def test_cot_not_visible_on_observation_tuesday(self):
        dates = pd.to_datetime(['2026-08-25', '2026-09-01'])
        source = pd.DataFrame({'date': dates, 'position': [10., 999.]})
        base = pd.DataFrame({'date': dates})
        joined = add_asof_feature_block(base, source, 'positioning')
        self.assertTrue(pd.isna(joined.position.iloc[0]))
        self.assertEqual(joined.position.iloc[1], 10.)

    def test_daily_close_not_assumed_known_same_day(self):
        self.assertEqual(available_dates(pd.Series(pd.to_datetime(['2026-09-01'])), 'spread').iloc[0],
                         pd.Timestamp('2026-09-02'))

    def test_baseline_can_win(self):
        actual = [1., -1.] * 20
        self.assertEqual(choose_model(actual, {'Zero Baseline': [0.] * 40, 'Bad': [100.] * 40}), 'Zero Baseline')

    def test_needs_history_and_material_improvement(self):
        self.assertEqual(choose_model([1.] * 25, {'Perfect': [1.] * 25}), 'Zero Baseline')
        self.assertEqual(choose_model([1.] * 30, {'Tiny improvement': [.01] * 30}), 'Zero Baseline')
        self.assertEqual(choose_model([1.] * 30, {'Better': [.5] * 30}), 'Better')

    def test_bad_predictions_never_selected(self):
        actual = [1.] * 30
        self.assertEqual(choose_model(actual, {'NaN': [np.nan] * 30, 'Incomplete': [1.]}), 'Zero Baseline')

    def test_future_results_cannot_change_prior_choice(self):
        actual = [1.] * 30 + [100.] * 30
        predictions = {'A': [.5] * 60, 'B': [100.] * 60}
        cutoff_choice = choose_model(actual[:30], {k: v[:30] for k, v in predictions.items()})
        self.assertEqual(cutoff_choice, 'A')
        actual[30:] = [-100000.] * 30
        self.assertEqual(choose_model(actual[:30], {k: v[:30] for k, v in predictions.items()}), cutoff_choice)


if __name__ == '__main__':
    unittest.main()
