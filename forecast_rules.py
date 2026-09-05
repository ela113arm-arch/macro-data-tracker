"""Fixed, auditable forecasting rules; no target-based feature search."""
import numpy as np
import pandas as pd

# Conservative publication buffers, not a replacement for archived data vintages.
# Monthly values are excluded from forecasting; these buffers also govern diagnostics.
WEEKLY = {'inventory', 'dos', 'balance', 'positioning', 'cftc', 'natgas_inventory', 'labor'}
MONTHLY = {'refinery', 'crude_production', 'cpi', 'ppi', 'sentiment', 'manufacturing',
           'housing', 'retail', 'transport', 'employment', 'jolts'}
FEATURES = [
    'composite_inventory_tightness', 'composite_positioning_crowding',
    'composite_macro_risk', 'composite_rates_pressure', 'composite_refining_margin',
    'etf_uso_volume_pressure', 'etf_ung_volume_pressure',
    'inventory_crude_US_chg_1', 'inventory_total_gasoline_US_chg_1',
    'inventory_distillate_US_chg_1', 'dos_crude_US_days',
    'balance_exports_crude_US', 'balance_spr_change_crude_US',
    'spread_brent_wti_spread',
]


def available_dates(dates, prefix):
    delay = 90 if prefix in MONTHLY else 7 if prefix in WEEKLY else 1
    return pd.to_datetime(dates) + pd.Timedelta(days=delay)


def choose_model(actual, predictions, minimum=26):
    """Use only already-realized errors. Require 5% RMSE improvement over flat."""
    actual = np.asarray(actual, dtype=float)
    if len(actual) < minimum:
        return 'Zero Baseline'
    baseline = float(np.sqrt(np.mean(actual ** 2)))
    eligible = []
    for name, values in predictions.items():
        pred = np.asarray(values, dtype=float)
        if len(pred) != len(actual) or not np.isfinite(pred).all():
            continue
        error = float(np.sqrt(np.mean((actual - pred) ** 2)))
        if error < baseline * 0.95:
            eligible.append((error, name))
    return min(eligible)[1] if eligible else 'Zero Baseline'
