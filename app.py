"""
Macro Data Tracker - Dashboard
Reads data from CSV files (run data_fetcher.py to update data)
"""

import os
import json
import shutil
from datetime import datetime, timedelta
from functools import lru_cache, wraps
from pathlib import Path
import subprocess
import sys
import threading
import time
import warnings

from flask import Flask, render_template, jsonify, request, Response, abort, send_from_directory
import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning

app = Flask(__name__)
APP_DIR = Path(__file__).parent
BUNDLED_DATA_DIR = APP_DIR / 'data'
DATA_DIR = Path(os.environ.get('DATA_DIR', BUNDLED_DATA_DIR))
SPR_REPORT_DIR = Path(os.environ.get('SPR_REPORT_DIR', APP_DIR / 'reports' / 'spr'))
LOCAL_SKLEARN_PACKAGES = APP_DIR / '.sklearn_packages'
if LOCAL_SKLEARN_PACKAGES.exists():
    sys.path.insert(0, str(LOCAL_SKLEARN_PACKAGES))
os.environ.setdefault('LOKY_MAX_CPU_COUNT', '8')
warnings.filterwarnings('ignore', category=PerformanceWarning)

# Cache configuration - cache invalidates every 5 minutes
CACHE_TTL = 300  # seconds
REFRESH_LOG = DATA_DIR / 'refresh.log'
REFRESH_LOCK = threading.Lock()
REFRESH_PROCESS = None
REFRESH_STATE = {
    'status': 'idle',
    'started_at': None,
    'finished_at': None,
    'returncode': None,
    'message': 'No refresh has been started.',
}
SPR_REFRESH_LOG = DATA_DIR / 'spr_refresh.log'
SPR_REFRESH_LOCK = threading.Lock()
SPR_REFRESH_PROCESS = None
SPR_REFRESH_STATE = {
    'status': 'idle',
    'started_at': None,
    'finished_at': None,
    'returncode': None,
    'message': 'No SPR refresh has been started.',
}
MODELING_CACHE = {
    'signature': None,
    'built_at': None,
    'payload': None,
}
MODELING_CACHE_FILE = APP_DIR / '.run' / 'energy_modeling_cache.json'
MODELING_BUILD_LOCK = threading.Lock()
MODELING_BUILD_STATE = {
    'status': 'idle',
    'started_at': None,
    'error': None,
}
DATA_SYNC_STATE = {
    'enabled': False,
    'status': 'not_run',
    'message': 'Bundled data sync has not run.',
    'bundled_data_dir': str(BUNDLED_DATA_DIR),
    'data_dir': str(DATA_DIR),
    'bundled_version': None,
    'data_dir_version': None,
    'files_copied': [],
}


def get_cache_key():
    """Return a cache key that changes every CACHE_TTL seconds"""
    return int(time.time() // CACHE_TTL)


def modeling_data_signature():
    """Fingerprint of the CSV data directory used to key the modeling cache"""
    return [
        [path.name, path.stat().st_mtime_ns, path.stat().st_size]
        for path in sorted(DATA_DIR.glob('*.csv'))
    ]


def start_modeling_build(force=False):
    """Build the modeling payload on a background thread.

    Returns False when a build is already running. The internal request goes
    through the normal endpoint so memory and disk caches are populated the
    same way as a user-triggered build.
    """
    if not MODELING_BUILD_LOCK.acquire(blocking=False):
        return False

    def runner():
        MODELING_BUILD_STATE.update({'status': 'building', 'started_at': time.time(), 'error': None})
        try:
            with app.test_client() as client:
                url = '/api/energy/modeling' + ('?refresh=1' if force else '')
                res = client.get(url)
            if res.status_code == 200:
                MODELING_BUILD_STATE.update({'status': 'ready', 'error': None})
            else:
                MODELING_BUILD_STATE.update({'status': 'error', 'error': f'Modeling build returned HTTP {res.status_code}'})
        except Exception as exc:
            MODELING_BUILD_STATE.update({'status': 'error', 'error': str(exc)})
        finally:
            MODELING_BUILD_LOCK.release()

    threading.Thread(target=runner, daemon=True, name='modeling-build').start()
    return True


def safe_endpoint(f):
    """Decorator for consistent error handling on API endpoints"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            data = f(*args, **kwargs)
            if isinstance(data, Response):
                return data
            if data is None:
                return jsonify({'error': 'No data available', 'endpoint': f.__name__}), 404
            return jsonify(data)
        except FileNotFoundError as e:
            return jsonify({'error': 'Data file not found', 'detail': str(e)}), 404
        except pd.errors.EmptyDataError:
            return jsonify({'error': 'Data file is empty'}), 404
        except Exception as e:
            return jsonify({'error': 'Internal server error', 'detail': str(e)}), 500
    return wrapper


@lru_cache(maxsize=64)
def read_csv_cached(filename, cache_key):
    """Read CSV file with caching - cache_key changes every 5 minutes"""
    filepath = DATA_DIR / filename
    if not filepath.exists():
        return None
    df = pd.read_csv(filepath)
    df = df.replace({np.nan: None})
    return df.to_dict('records')


def read_csv(filename):
    """Read CSV file with automatic cache invalidation"""
    return read_csv_cached(filename, get_cache_key())


def read_dataframe(filename):
    """Read a data CSV as a pandas DataFrame for server-side analysis."""
    filepath = DATA_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(filepath)
    return pd.read_csv(filepath)


def to_float(value):
    """Convert numeric-like values to finite floats, otherwise None."""
    if value is None or value == '':
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if np.isfinite(numeric) else None


def clean_json(value):
    """Recursively replace numpy/pandas scalars and NaNs with JSON-safe values."""
    if isinstance(value, dict):
        return {key: clean_json(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean_json(item) for item in value]
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime('%Y-%m-%d')
    if pd.isna(value):
        return None
    return value


def prepare_date_frame(df, date_col='date'):
    """Normalize and sort a dated frame."""
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    return df.dropna(subset=[date_col]).sort_values(date_col)


def rolling_last_percentile(series, window, min_periods=None):
    """Trailing percentile rank of the latest value in each rolling window."""
    min_periods = min_periods or max(4, window // 2)
    return series.rolling(window, min_periods=min_periods).apply(
        lambda values: pd.Series(values).rank(pct=True).iloc[-1],
        raw=False,
    )


def add_derived_feature_block(df, date_col, columns, prefix):
    """Create level, change, z-score, and percentile features from source columns."""
    source = prepare_date_frame(df, date_col)
    out = source[[date_col]].rename(columns={date_col: 'date'}).copy()
    for col in columns:
        if col not in source.columns:
            continue
        values = pd.to_numeric(source[col], errors='coerce')
        key = f'{prefix}_{col}'
        out[key] = values
        out[f'{key}_chg_1'] = values.diff(1)
        out[f'{key}_chg_4'] = values.diff(4)
        out[f'{key}_chg_13'] = values.diff(13)
        out[f'{key}_z_13'] = (values - values.rolling(13, min_periods=6).mean()) / values.rolling(13, min_periods=6).std()
        out[f'{key}_z_26'] = (values - values.rolling(26, min_periods=10).mean()) / values.rolling(26, min_periods=10).std()
        out[f'{key}_pctile_26'] = rolling_last_percentile(values, 26, 10)
    return out


def add_asof_feature_block(base, source, prefix):
    """Backward as-of join source features into COT Tuesday rows."""
    feature_cols = [col for col in source.columns if col != 'date']
    if not feature_cols:
        return base
    source = source.sort_values('date').rename(columns={'date': f'{prefix}_source_date'})
    merged = pd.merge_asof(
        base.sort_values('date'),
        source,
        left_on='date',
        right_on=f'{prefix}_source_date',
        direction='backward',
    )
    return merged


def schema_for_csv(name, description):
    """Summarize a CSV schema for the stats dashboard."""
    df = read_dataframe(name)
    date_column = next((col for col in df.columns if col.lower() in ('date', 'period', 'record_date')), None)
    date_range = {'min': None, 'max': None}
    if date_column:
        dates = pd.to_datetime(df[date_column], errors='coerce')
        if dates.notna().any():
            date_range = {
                'min': dates.min().strftime('%Y-%m-%d'),
                'max': dates.max().strftime('%Y-%m-%d'),
            }
    return {
        'file': name,
        'description': description,
        'rows': int(len(df)),
        'columns': int(len(df.columns)),
        'date_column': date_column,
        'date_range': date_range,
        'fields': [
            {
                'name': col,
                'dtype': str(df[col].dtype),
                'missing_pct': round(float(df[col].isna().mean() * 100), 1),
                'non_null': int(df[col].notna().sum()),
            }
            for col in df.columns
        ],
    }



def parse_timestamp(value):
    """Parse a CSV timestamp into a comparable datetime, returning None on blanks/errors."""
    if value is None or pd.isna(value):
        return None
    try:
        parsed = pd.to_datetime(value, errors='coerce')
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()
    except (TypeError, ValueError):
        return None


def data_dir_version(data_dir):
    """Return the newest refresh timestamp recorded by a data directory."""
    candidates = []

    metadata_file = data_dir / 'metadata.csv'
    if metadata_file.exists():
        try:
            metadata = pd.read_csv(metadata_file, nrows=1)
            if 'last_updated' in metadata.columns and not metadata.empty:
                candidates.append(parse_timestamp(metadata.iloc[0]['last_updated']))
        except (OSError, pd.errors.EmptyDataError, ValueError):
            pass

    spr_summary_file = data_dir / 'spr_release_summary.csv'
    if spr_summary_file.exists():
        try:
            spr_summary = pd.read_csv(spr_summary_file, usecols=lambda col: col == 'refreshed_at')
            if 'refreshed_at' in spr_summary.columns:
                candidates.extend(parse_timestamp(value) for value in spr_summary['refreshed_at'])
        except (OSError, pd.errors.EmptyDataError, ValueError):
            pass

    candidates = [candidate for candidate in candidates if candidate is not None]
    return max(candidates) if candidates else None


def should_sync_bundled_data():
    """Return whether the bundled image data should seed an external DATA_DIR."""
    setting = os.environ.get('SYNC_BUNDLED_DATA_ON_STARTUP', '1').strip().lower()
    return setting not in {'0', 'false', 'no', 'off'}


def sync_bundled_data_if_newer():
    """
    Seed an external DATA_DIR from the data bundled into the deployed image.

    Render persistent disks are mounted outside the Git checkout. If DATA_DIR points at
    such a disk, newly deployed CSVs from GitHub are otherwise hidden by the old disk
    contents. This copies the bundled CSVs only when their recorded refresh timestamp is
    newer than the external DATA_DIR's recorded timestamp, preserving fresher in-app
    refreshes.
    """
    global DATA_SYNC_STATE

    state = {
        'enabled': False,
        'status': 'skipped',
        'message': 'Bundled data sync skipped.',
        'bundled_data_dir': str(BUNDLED_DATA_DIR),
        'data_dir': str(DATA_DIR),
        'bundled_version': None,
        'data_dir_version': None,
        'files_copied': [],
    }

    if not should_sync_bundled_data():
        state['message'] = 'SYNC_BUNDLED_DATA_ON_STARTUP is disabled.'
        DATA_SYNC_STATE = state
        return state

    try:
        if DATA_DIR.resolve() == BUNDLED_DATA_DIR.resolve():
            state['message'] = 'DATA_DIR is the bundled repository data directory.'
            DATA_SYNC_STATE = state
            return state
    except OSError:
        pass

    state['enabled'] = True
    bundled_version = data_dir_version(BUNDLED_DATA_DIR)
    target_version = data_dir_version(DATA_DIR)
    state['bundled_version'] = bundled_version.isoformat() if bundled_version else None
    state['data_dir_version'] = target_version.isoformat() if target_version else None

    if not BUNDLED_DATA_DIR.exists():
        state['status'] = 'failed'
        state['message'] = 'Bundled data directory does not exist.'
        DATA_SYNC_STATE = state
        return state

    should_copy_all = bundled_version is not None and (
        target_version is None or bundled_version > target_version
    )
    if not should_copy_all:
        missing_files = [
            source for source in BUNDLED_DATA_DIR.glob('*.csv')
            if not (DATA_DIR / source.name).exists()
        ]
        if not missing_files:
            state['message'] = 'External DATA_DIR is current or newer than bundled data.'
            DATA_SYNC_STATE = state
            return state
        sources_to_copy = missing_files
    else:
        sources_to_copy = list(BUNDLED_DATA_DIR.glob('*.csv'))

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        copied = []
        for source in sources_to_copy:
            destination = DATA_DIR / source.name
            shutil.copy2(source, destination)
            copied.append(source.name)
        read_csv_cached.cache_clear()
        state['status'] = 'completed'
        state['message'] = f'Copied {len(copied)} bundled CSV file(s) into DATA_DIR.'
        state['files_copied'] = copied
    except OSError as exc:
        state['status'] = 'failed'
        state['message'] = f'Bundled data sync failed: {exc}'

    DATA_SYNC_STATE = state
    return state

def now_iso():
    """Current local timestamp for API responses."""
    return datetime.now().isoformat()


def refresh_snapshot_locked():
    """Return refresh state, updating it if the child process has exited."""
    global REFRESH_PROCESS

    if REFRESH_PROCESS is not None:
        returncode = REFRESH_PROCESS.poll()
        if returncode is None:
            REFRESH_STATE['status'] = 'running'
        else:
            REFRESH_STATE['finished_at'] = now_iso()
            REFRESH_STATE['returncode'] = returncode
            if returncode == 0:
                REFRESH_STATE['status'] = 'completed'
                REFRESH_STATE['message'] = 'Data refresh completed.'
                read_csv_cached.cache_clear()
            else:
                REFRESH_STATE['status'] = 'failed'
                REFRESH_STATE['message'] = f'Data refresh failed with exit code {returncode}.'
            REFRESH_PROCESS = None

    snapshot = dict(REFRESH_STATE)
    snapshot['is_running'] = snapshot['status'] == 'running'
    snapshot['log_file'] = str(REFRESH_LOG)
    return snapshot


def get_refresh_snapshot():
    """Thread-safe refresh state snapshot."""
    with REFRESH_LOCK:
        return refresh_snapshot_locked()


def spr_refresh_snapshot_locked():
    """Return SPR refresh state, updating it if the child process has exited."""
    global SPR_REFRESH_PROCESS

    if SPR_REFRESH_PROCESS is not None:
        returncode = SPR_REFRESH_PROCESS.poll()
        if returncode is None:
            SPR_REFRESH_STATE['status'] = 'running'
        else:
            SPR_REFRESH_STATE['finished_at'] = now_iso()
            SPR_REFRESH_STATE['returncode'] = returncode
            if returncode == 0:
                SPR_REFRESH_STATE['status'] = 'completed'
                SPR_REFRESH_STATE['message'] = 'SPR data refresh completed.'
                read_csv_cached.cache_clear()
            else:
                SPR_REFRESH_STATE['status'] = 'failed'
                SPR_REFRESH_STATE['message'] = f'SPR data refresh failed with exit code {returncode}.'
            SPR_REFRESH_PROCESS = None

    snapshot = dict(SPR_REFRESH_STATE)
    snapshot['is_running'] = snapshot['status'] == 'running'
    snapshot['log_file'] = str(SPR_REFRESH_LOG)
    return snapshot


def get_spr_refresh_snapshot():
    """Thread-safe SPR refresh state snapshot."""
    with SPR_REFRESH_LOCK:
        return spr_refresh_snapshot_locked()


sync_bundled_data_if_newer()


@app.route('/')
def index():
    return render_template('stats.html')


@app.route('/macro')
def macro_dashboard():
    return render_template('index.html')


@app.route('/spr')
def spr_dashboard():
    return render_template('spr.html')


@app.route('/spr/info')
def spr_info_page():
    return render_template('spr_info.html')


@app.route('/spr/buyer-awards')
def spr_buyer_awards_report():
    return send_from_directory(SPR_REPORT_DIR, 'spr_buyer_awards_report_latest.html')


@app.route('/reports/spr/<path:filename>')
def spr_report_file(filename):
    if '/' in filename or '\\' in filename:
        abort(404)
    if not filename.startswith('spr_') or Path(filename).suffix.lower() not in {'.html', '.md'}:
        abort(404)
    return send_from_directory(SPR_REPORT_DIR, filename)


@app.route('/api/gdp/components')
@safe_endpoint
def get_gdp_components():
    """GDP and components data"""
    return read_csv('gdp_components.csv')


@app.route('/api/gdp-contributions')
@safe_endpoint
def get_gdp_contributions():
    """GDP growth contributions"""
    return read_csv('gdp_contributions.csv')


@app.route('/api/trade/detail')
@safe_endpoint
def get_trade_detail():
    """Trade breakdown (goods vs services)"""
    return read_csv('trade_detail.csv')


@app.route('/api/trade/categories')
@safe_endpoint
def get_trade_categories():
    """All trade categories (imports, exports, balances)"""
    return read_csv('trade_categories.csv')


@app.route('/api/trade/detailed')
@safe_endpoint
def get_detailed_trade():
    """Detailed trade categories from BEA ITA (pharma, metals, semiconductors)"""
    return read_csv('detailed_trade.csv')


@app.route('/api/trade/services')
@safe_endpoint
def get_services_trade():
    """Services trade breakdown"""
    return read_csv('services_trade.csv')


@app.route('/api/commodities')
@safe_endpoint
def get_commodities():
    """Commodity prices"""
    return read_csv('commodities.csv')


@app.route('/api/petroleum/inventories')
@safe_endpoint
def get_petroleum_inventories():
    """Weekly petroleum inventories by product and PADD (last 5 years)"""
    data = read_csv('petroleum_inventories.csv')
    if data:
        cutoff = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
        data = [row for row in data if row.get('date', '') >= cutoff]
    return data


@app.route('/api/petroleum/metadata')
@safe_endpoint
def get_petroleum_metadata():
    """Metadata for petroleum inventory series"""
    return read_csv('petroleum_inventories_metadata.csv')


@app.route('/api/petroleum/supply-demand')
@safe_endpoint
def get_supply_demand():
    """Monthly supply/demand data from EIA PSM"""
    return read_csv('supply_demand.csv')


@app.route('/api/petroleum/weekly-balance')
@safe_endpoint
def get_weekly_balance():
    """Weekly supply/demand balance from EIA WPSR for all products"""
    return read_csv('weekly_balance.csv')


@app.route('/api/petroleum/crude-production')
@safe_endpoint
def get_crude_production():
    """Monthly U.S. crude oil production and supply adjustment from EIA PSM"""
    return read_csv('crude_production.csv')


@app.route('/api/petroleum/days-of-supply')
@safe_endpoint
def get_days_of_supply():
    """Days of supply for petroleum products"""
    return read_csv('days_of_supply.csv')


@app.route('/api/petroleum/crack-spreads')
@safe_endpoint
def get_crack_spreads():
    """Crack spreads (gasoline, heating oil vs crude)"""
    return read_csv('crack_spreads.csv')


@app.route('/api/rig-count')
@safe_endpoint
def get_rig_count():
    """Baker Hughes rig count data"""
    return read_csv('rig_count.csv')


@app.route('/api/energy/upstream-activity')
@safe_endpoint
def get_upstream_activity():
    """WTI, frac spreads, oil rigs, and DUC wells for the upstream activity chart"""
    return read_csv('upstream_activity.csv')


@app.route('/api/cftc-positioning')
@safe_endpoint
def get_cftc_positioning():
    """CFTC Commitment of Traders positioning data"""
    return read_csv('cftc_positioning.csv')


@app.route('/api/cot/brent-positioning')
@safe_endpoint
def get_cot_brent_positioning():
    """WTI + Brent managed money COT positioning aligned with Brent Tuesday closes"""
    return read_csv('cot_wti_brent_merged.csv')


@app.route('/api/energy/total-inv-eia')
@safe_endpoint
def get_total_inv_eia():
    """Total EIA petroleum stocks NTPS vs WTI fair value dataset"""
    return read_csv('total_inv_eia_fair_value.csv')


@app.route('/api/spr/summary')
@safe_endpoint
def get_spr_summary():
    """SPR release headline metrics."""
    return read_csv('spr_release_summary.csv')


@app.route('/api/spr/weekly-inventory')
@safe_endpoint
def get_spr_weekly_inventory():
    """EIA WCSSTUS1 weekly SPR inventory and drawdown metrics."""
    return read_csv('spr_weekly_inventory.csv')


@app.route('/api/spr/monthly')
@safe_endpoint
def get_spr_monthly():
    """Monthly SPR release and observed drawdown metrics."""
    return read_csv('spr_monthly_summary.csv')


@app.route('/api/spr/events')
@safe_endpoint
def get_spr_events():
    """Structured DOE/IEA SPR release events."""
    return read_csv('spr_release_events.csv')


@app.route('/api/spr/site-quality')
@safe_endpoint
def get_spr_site_quality():
    """DOE SPR site and quality inventory snapshot."""
    return read_csv('spr_site_quality.csv')


@app.route('/api/spr/site-info')
@safe_endpoint
def get_spr_site_info():
    """SPR storage site infrastructure, inventory, and distribution facts."""
    return read_csv('spr_site_info.csv')


@app.route('/api/spr/release-quality')
@safe_endpoint
def get_spr_release_quality():
    """SPR release volume by public quality classification."""
    return read_csv('spr_release_quality.csv')


@app.route('/api/spr/release-buyers')
@safe_endpoint
def get_spr_release_buyers():
    """SPR release award volumes by public buyer award information."""
    return read_csv('spr_release_buyers.csv')


@app.route('/api/spr/buyer-awards')
@safe_endpoint
def get_spr_buyer_awards():
    """Normalized SPR buyer awards joined to tranche delivery windows."""
    return read_csv('spr_buyer_award_delivery_windows.csv')


@app.route('/api/spr/news')
@safe_endpoint
def get_spr_news():
    """Latest official SPR release source rows."""
    return read_csv('spr_news.csv')


@app.route('/api/energy/exploration')
@safe_endpoint
def get_energy_exploration():
    """Exploratory energy feature set with Brent Tuesday-close weekly change as y."""
    csv_files = {
        'cot_wti_brent_merged.csv': 'Tuesday COT positioning and Brent/WTI Tuesday closes',
        'cftc_positioning.csv': 'CFTC energy positioning plus USO/UNG ETF prices and volume',
        'petroleum_inventories.csv': 'Weekly EIA petroleum inventories',
        'days_of_supply.csv': 'Weekly inventory days of supply',
        'refinery_runs.csv': 'Monthly refinery crude inputs',
        'crack_spreads.csv': 'Daily crack spreads and Brent-WTI spread',
        'market_prices.csv': 'Daily futures and macro market prices',
        'weekly_balance.csv': 'Weekly EIA production/import/export/refinery/product supplied balance',
        'total_inv_eia_fair_value.csv': 'EIA WTESTUS1 NTPS and CPI-adjusted WTI fair value',
    }

    def schema_for(name, description):
        df = read_dataframe(name)
        date_column = next((col for col in df.columns if col.lower() in ('date', 'period')), None)
        date_range = {'min': None, 'max': None}
        if date_column:
            dates = pd.to_datetime(df[date_column], errors='coerce')
            if dates.notna().any():
                date_range = {
                    'min': dates.min().strftime('%Y-%m-%d'),
                    'max': dates.max().strftime('%Y-%m-%d'),
                }
        return {
            'file': name,
            'description': description,
            'rows': int(len(df)),
            'columns': int(len(df.columns)),
            'date_column': date_column,
            'date_range': date_range,
            'fields': [
                {
                    'name': col,
                    'dtype': str(df[col].dtype),
                    'missing_pct': round(float(df[col].isna().mean() * 100), 1),
                    'non_null': int(df[col].notna().sum()),
                }
                for col in df.columns
            ],
        }

    def prepare_date_frame(df, date_col='date'):
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col]).sort_values(date_col)
        return df

    def add_asof_features(base, source, date_col, feature_map, prefix=None):
        working = prepare_date_frame(source, date_col)
        rename_map = {source_col: target_col for source_col, target_col in feature_map.items() if source_col in working.columns}
        if not rename_map:
            return base
        keep = [date_col] + list(rename_map.keys())
        working = working[keep].rename(columns={date_col: 'source_date', **rename_map})
        if prefix:
            working = working.rename(columns={col: f'{prefix}_{col}' for col in rename_map.values()})
        merged = pd.merge_asof(
            base.sort_values('date'),
            working.sort_values('source_date'),
            left_on='date',
            right_on='source_date',
            direction='backward',
        )
        if prefix:
            merged = merged.rename(columns={'source_date': f'{prefix}_source_date'})
        else:
            merged = merged.rename(columns={'source_date': 'source_date'})
        return merged

    def calc_regression(x, y):
        pair = pd.DataFrame({'x': x, 'y': y}).replace([np.inf, -np.inf], np.nan).dropna()
        n = len(pair)
        if n < 8 or pair['x'].nunique() < 2:
            return {'n': n, 'corr': None, 'beta': None, 'alpha': None, 'r2': None, 't_stat': None}
        x_values = pair['x'].astype(float)
        y_values = pair['y'].astype(float)
        x_mean = x_values.mean()
        y_mean = y_values.mean()
        ss_x = float(((x_values - x_mean) ** 2).sum())
        ss_y = float(((y_values - y_mean) ** 2).sum())
        cov_xy = float(((x_values - x_mean) * (y_values - y_mean)).sum())
        beta = cov_xy / ss_x if ss_x else np.nan
        alpha = y_mean - beta * x_mean
        corr = cov_xy / np.sqrt(ss_x * ss_y) if ss_x and ss_y else np.nan
        residuals = y_values - (alpha + beta * x_values)
        rss = float((residuals ** 2).sum())
        r2 = 1 - (rss / ss_y) if ss_y else np.nan
        stderr = np.sqrt(rss / (n - 2) / ss_x) if n > 2 and ss_x else np.nan
        t_stat = beta / stderr if stderr and np.isfinite(stderr) else np.nan
        return {
            'n': int(n),
            'corr': to_float(corr),
            'beta': to_float(beta),
            'alpha': to_float(alpha),
            'r2': to_float(r2),
            't_stat': to_float(t_stat),
        }

    cot = prepare_date_frame(read_dataframe('cot_wti_brent_merged.csv'))
    base = cot.rename(columns={
        'brent_ww': 'y_brent_ww',
        'brent_close': 'brent_tuesday_close',
        'wti_close': 'wti_tuesday_close',
        'wti_ww': 'wti_tuesday_ww',
    })[
        [
            'date', 'y_brent_ww', 'brent_tuesday_close', 'wti_tuesday_close',
            'wti_tuesday_ww', 'WTI_CME_mm_net', 'BRENT_ICE_mm_net',
            'COMBINED_mm_net', 'COMBINED_mm_net_ww'
        ]
    ].dropna(subset=['date', 'y_brent_ww']).sort_values('date')
    base['cot_weekday'] = base['date'].dt.day_name()
    base['brent_tuesday_return_pct'] = base['brent_tuesday_close'].pct_change() * 100
    base['combined_mm_net_4w_change'] = base['COMBINED_mm_net'].diff(4)
    base['combined_mm_net_13w_z'] = (
        (base['COMBINED_mm_net'] - base['COMBINED_mm_net'].rolling(13, min_periods=8).mean())
        / base['COMBINED_mm_net'].rolling(13, min_periods=8).std()
    )

    crack = prepare_date_frame(read_dataframe('crack_spreads.csv'))
    crack_features = crack[['date', 'brent', 'wti', 'crack_321', 'gasoline_crack', 'ho_crack', 'brent_wti_spread']].copy()
    for col in ['brent', 'wti', 'crack_321', 'gasoline_crack', 'ho_crack', 'brent_wti_spread']:
        crack_features[f'{col}_5d_change'] = crack_features[col].diff(5)
        crack_features[f'{col}_20d_z'] = (
            (crack_features[col] - crack_features[col].rolling(20, min_periods=12).mean())
            / crack_features[col].rolling(20, min_periods=12).std()
        )
    base = add_asof_features(
        base,
        crack_features,
        'date',
        {col: col for col in crack_features.columns if col != 'date'},
        'daily',
    )

    prices = prepare_date_frame(read_dataframe('market_prices.csv'))
    price_features = prices[['date', 'crude_oil', 'nat_gas', 'sp500', 'gold', 'silver', 'copper']].copy()
    for col in ['crude_oil', 'nat_gas', 'sp500', 'gold', 'silver', 'copper']:
        price_features[f'{col}_5d_change'] = price_features[col].diff(5)
        price_features[f'{col}_5d_return_pct'] = price_features[col].pct_change(5) * 100
    base = add_asof_features(
        base,
        price_features,
        'date',
        {col: col for col in price_features.columns if col != 'date'},
        'market',
    )

    inventories = prepare_date_frame(read_dataframe('petroleum_inventories.csv'))
    inventory_features = inventories[['date', 'crude_US', 'total_gasoline_US', 'distillate_US', 'propane_US', 'ngl_US']].copy()
    for col in ['crude_US', 'total_gasoline_US', 'distillate_US', 'propane_US', 'ngl_US']:
        inventory_features[f'{col}_wow'] = inventory_features[col].diff()
        inventory_features[f'{col}_4w_change'] = inventory_features[col].diff(4)
        inventory_features[f'{col}_52w_z'] = (
            (inventory_features[col] - inventory_features[col].rolling(52, min_periods=26).mean())
            / inventory_features[col].rolling(52, min_periods=26).std()
        )
    base = add_asof_features(
        base,
        inventory_features,
        'date',
        {col: col for col in inventory_features.columns if col != 'date'},
        'inv',
    )

    dos = prepare_date_frame(read_dataframe('days_of_supply.csv'))
    dos_cols = [
        'crude_US_days', 'gasoline_US_days', 'distillate_US_days',
        'crude_US_days_vs_avg', 'gasoline_US_days_vs_avg', 'distillate_US_days_vs_avg'
    ]
    dos_features = dos[['date'] + [col for col in dos_cols if col in dos.columns]].copy()
    for col in [col for col in dos_cols if col in dos_features.columns]:
        dos_features[f'{col}_wow'] = dos_features[col].diff()
    base = add_asof_features(
        base,
        dos_features,
        'date',
        {col: col for col in dos_features.columns if col != 'date'},
        'dos',
    )

    refinery = prepare_date_frame(read_dataframe('refinery_runs.csv'))
    refinery['crude_input_mom'] = refinery['crude_input'].diff()
    refinery['crude_input_yoy'] = refinery['crude_input'].diff(12)
    base = add_asof_features(
        base,
        refinery[['date', 'crude_input', 'crude_input_mom', 'crude_input_yoy']],
        'date',
        {
            'crude_input': 'crude_input',
            'crude_input_mom': 'crude_input_mom',
            'crude_input_yoy': 'crude_input_yoy',
        },
        'refinery',
    )

    balance = read_dataframe('weekly_balance.csv')
    balance['date'] = pd.to_datetime(balance['period'], errors='coerce')
    balance_wide = balance.pivot_table(
        index='date',
        columns='product',
        values=['production', 'imports', 'exports', 'refinery_input', 'refinery_output', 'product_supplied'],
        aggfunc='last',
    )
    balance_wide.columns = [f'{metric}_{product}' for metric, product in balance_wide.columns]
    balance_wide = balance_wide.reset_index().sort_values('date')
    for col in list(balance_wide.columns):
        if col != 'date':
            balance_wide[f'{col}_wow'] = balance_wide[col].diff()
    base = add_asof_features(
        base,
        balance_wide,
        'date',
        {col: col for col in balance_wide.columns if col != 'date'},
        'bal',
    )

    cftc = prepare_date_frame(read_dataframe('cftc_positioning.csv'))
    etf = cftc[['date', 'uso_price', 'uso_volume', 'ung_price', 'ung_volume']].copy()
    for prefix in ['uso', 'ung']:
        etf[f'{prefix}_volume_ma50'] = etf[f'{prefix}_volume'].rolling(50, min_periods=25).mean()
        etf[f'{prefix}_volume_pressure'] = (etf[f'{prefix}_volume'] / etf[f'{prefix}_volume_ma50']) - 1
        etf[f'{prefix}_price_5d_return_pct'] = etf[f'{prefix}_price'].pct_change(5) * 100
    etf = etf.set_index('date')
    weekly_etf = pd.DataFrame(index=base['date'])
    for cot_date in base['date']:
        window = etf.loc[(etf.index <= cot_date) & (etf.index >= cot_date - pd.Timedelta(days=6))]
        for col in ['uso_volume_pressure', 'ung_volume_pressure', 'uso_price_5d_return_pct', 'ung_price_5d_return_pct']:
            weekly_etf.loc[cot_date, col] = window[col].mean() if col in window and not window.empty else np.nan
    weekly_etf = weekly_etf.reset_index().rename(columns={'index': 'date'})
    base = base.merge(weekly_etf, on='date', how='left')

    cftc_weekly = cftc.dropna(subset=['wti_oil_mm_net']).copy()
    cftc_weekly['wti_oil_mm_net_wow'] = cftc_weekly['wti_oil_mm_net'].diff()
    base = add_asof_features(
        base,
        cftc_weekly[['date', 'wti_oil_mm_net', 'wti_oil_mm_net_pct', 'wti_oil_mm_net_wow', 'nat_gas_mm_net', 'rbob_gas_mm_net']],
        'date',
        {
            'wti_oil_mm_net': 'wti_oil_mm_net',
            'wti_oil_mm_net_pct': 'wti_oil_mm_net_pct',
            'wti_oil_mm_net_wow': 'wti_oil_mm_net_wow',
            'nat_gas_mm_net': 'nat_gas_mm_net',
            'rbob_gas_mm_net': 'rbob_gas_mm_net',
        },
        'cftc',
    )

    base = base.sort_values('date')
    excluded_predictors = {
        'brent_tuesday_close',
        'brent_tuesday_return_pct',
        'wti_tuesday_close',
        'wti_tuesday_ww',
        'daily_brent',
        'daily_brent_5d_change',
        'daily_brent_20d_z',
        'daily_wti',
        'daily_wti_5d_change',
        'daily_wti_20d_z',
        'market_crude_oil',
        'market_crude_oil_5d_change',
        'market_crude_oil_5d_return_pct',
        'uso_price_5d_return_pct',
        'ung_price_5d_return_pct',
    }
    numeric_cols = [
        col for col in base.select_dtypes(include=[np.number]).columns
        if col != 'y_brent_ww' and not col.endswith('_source_date') and col not in excluded_predictors
    ]

    relationships = []
    for col in numeric_cols:
        if base[col].notna().sum() < 20:
            continue
        lag_stats = []
        for lag in range(0, 5):
            stats = calc_regression(base[col].shift(lag), base['y_brent_ww'])
            lag_stats.append({'lag_weeks': lag, **stats})
        valid = [item for item in lag_stats if item['corr'] is not None]
        if not valid:
            continue
        best = max(valid, key=lambda item: abs(item['corr']))
        latest = base[col].dropna().iloc[-1] if base[col].notna().any() else np.nan
        relationships.append({
            'feature': col,
            'n': best['n'],
            'best_lag_weeks': best['lag_weeks'],
            'corr': best['corr'],
            'r2': best['r2'],
            'beta': best['beta'],
            't_stat': best['t_stat'],
            'latest': to_float(latest),
            'lag0_corr': lag_stats[0]['corr'],
            'lag1_corr': lag_stats[1]['corr'],
            'lag2_corr': lag_stats[2]['corr'],
            'lag3_corr': lag_stats[3]['corr'],
            'lag4_corr': lag_stats[4]['corr'],
        })
    relationships = sorted(relationships, key=lambda item: abs(item['corr'] or 0), reverse=True)[:45]

    top_features = [item['feature'] for item in relationships[:8]]
    rolling = []
    for col in top_features:
        corr = base[col].rolling(26, min_periods=16).corr(base['y_brent_ww'])
        rolling.append({
            'feature': col,
            'dates': base['date'].dt.strftime('%Y-%m-%d').tolist(),
            'values': [to_float(value) for value in corr.tolist()],
        })

    y = base['y_brent_ww'].dropna()
    y_summary = {
        'observations': int(y.count()),
        'start_date': base['date'].min().strftime('%Y-%m-%d'),
        'end_date': base['date'].max().strftime('%Y-%m-%d'),
        'latest': to_float(y.iloc[-1]),
        'mean': to_float(y.mean()),
        'median': to_float(y.median()),
        'std': to_float(y.std()),
        'positive_hit_rate': to_float((y > 0).mean() * 100),
        'p10': to_float(y.quantile(0.10)),
        'p90': to_float(y.quantile(0.90)),
    }

    regime_features = {
        'COMBINED_mm_net_ww': 'MM net length added',
        'daily_crack_321_5d_change': '3-2-1 crack widening',
        'daily_brent_wti_spread_5d_change': 'Brent-WTI widening',
        'inv_crude_US_wow': 'Crude inventory build',
        'inv_total_gasoline_US_wow': 'Gasoline inventory build',
        'dos_crude_US_days_wow': 'Crude days supply rising',
        'uso_volume_pressure': 'USO volume pressure high',
    }
    regimes = []
    for weeks in [4, 8, 13, 26]:
        subset = base.tail(weeks)
        regimes.append({
            'window_weeks': weeks,
            'avg_y_brent_ww': to_float(subset['y_brent_ww'].mean()),
            'sum_y_brent_ww': to_float(subset['y_brent_ww'].sum()),
            'positive_weeks': int((subset['y_brent_ww'] > 0).sum()),
            'negative_weeks': int((subset['y_brent_ww'] < 0).sum()),
            'signals': [
                {
                    'feature': col,
                    'label': label,
                    'avg': to_float(subset[col].mean()) if col in subset else None,
                    'latest': to_float(subset[col].dropna().iloc[-1]) if col in subset and subset[col].notna().any() else None,
                    'positive_share': to_float((subset[col] > 0).mean() * 100) if col in subset else None,
                }
                for col, label in regime_features.items()
            ],
        })

    last_rows = base.tail(12).copy()
    last_rows['date'] = last_rows['date'].dt.strftime('%Y-%m-%d')
    display_cols = [
        'date', 'y_brent_ww', 'brent_tuesday_close', 'COMBINED_mm_net_ww',
        'COMBINED_mm_net', 'daily_crack_321', 'daily_brent_wti_spread',
        'inv_crude_US_wow', 'inv_total_gasoline_US_wow', 'dos_crude_US_days',
        'uso_volume_pressure'
    ]

    feature_docs = [
        {'field': 'y_brent_ww', 'formula': 'Current Brent Tuesday close minus prior COT Tuesday Brent close', 'source': 'cot_wti_brent_merged.csv', 'alignment': 'Tuesday COT report date'},
        {'field': 'COMBINED_mm_net_ww', 'formula': 'Weekly change in WTI CME + Brent ICE managed-money net length, million barrels', 'source': 'cot_wti_brent_merged.csv', 'alignment': 'Tuesday COT report date'},
        {'field': 'combined_mm_net_4w_change / combined_mm_net_13w_z', 'formula': 'Combined managed-money net length minus four COT weeks earlier / 13-week z-score of combined net length', 'source': 'cot_wti_brent_merged.csv', 'alignment': 'Tuesday COT report date'},
        {'field': 'daily_spread/crack_*_5d_change', 'formula': 'Crack-spread or Brent-WTI spread as of the latest observation on or before Tuesday minus value five trading rows earlier; outright Brent, WTI, and crude-oil price changes are excluded as predictors', 'source': 'crack_spreads.csv', 'alignment': 'As-of COT Tuesday'},
        {'field': 'daily_spread/crack_*_20d_z', 'formula': '(Crack-spread or Brent-WTI spread - trailing 20-observation mean) / trailing 20-observation standard deviation; outright oil prices are excluded as predictors', 'source': 'crack_spreads.csv', 'alignment': 'As-of COT Tuesday'},
        {'field': 'inv_*_wow', 'formula': 'Latest weekly EIA stock level on or before Tuesday minus prior weekly stock level', 'source': 'petroleum_inventories.csv', 'alignment': 'Latest weekly period available by COT Tuesday'},
        {'field': 'inv_*_4w_change', 'formula': 'Latest weekly EIA stock level minus the level four weekly rows earlier', 'source': 'petroleum_inventories.csv', 'alignment': 'Latest weekly period available by COT Tuesday'},
        {'field': 'inv_*_52w_z', 'formula': '(Weekly stock level - trailing 52-week mean) / trailing 52-week standard deviation', 'source': 'petroleum_inventories.csv', 'alignment': 'Latest weekly period available by COT Tuesday'},
        {'field': 'dos_*_wow', 'formula': 'Latest days-of-supply metric minus prior weekly row', 'source': 'days_of_supply.csv', 'alignment': 'Latest weekly period available by COT Tuesday'},
        {'field': 'refinery_crude_input_mom / yoy', 'formula': 'Monthly refinery crude input minus prior month / prior 12th month', 'source': 'refinery_runs.csv', 'alignment': 'Latest monthly observation on or before COT Tuesday'},
        {'field': 'bal_*_wow', 'formula': 'Weekly balance metric by product minus prior weekly row after product pivot', 'source': 'weekly_balance.csv', 'alignment': 'Latest weekly period available by COT Tuesday'},
        {'field': 'uso_volume_pressure / ung_volume_pressure', 'formula': 'Average over Tuesday-ending week of daily volume / trailing 50-day volume average - 1', 'source': 'cftc_positioning.csv', 'alignment': 'Tuesday-ending six-calendar-day window'},
        {'field': 'lag correlations', 'formula': 'Feature shifted 0-4 COT weeks to test whether prior values explain current y_brent_ww', 'source': 'derived analysis frame', 'alignment': 'COT Tuesday weekly rows'},
    ]

    return clean_json({
        'schemas': [schema_for(name, desc) for name, desc in csv_files.items()],
        'y_summary': y_summary,
        'relationships': relationships,
        'rolling_correlations': rolling,
        'regimes': regimes,
        'y_series': [
            {'date': row['date'].strftime('%Y-%m-%d'), 'y_brent_ww': to_float(row['y_brent_ww'])}
            for _, row in base[['date', 'y_brent_ww']].dropna().iterrows()
        ],
        'last_rows': last_rows[[col for col in display_cols if col in last_rows.columns]].to_dict('records'),
        'feature_docs': feature_docs,
        'analysis_columns': list(base.columns),
    })


@app.route('/api/energy/modeling')
@safe_endpoint
def get_energy_modeling():
    """Comprehensive forward-looking Brent modeling dashboard payload."""
    refresh = request.args.get('refresh') == '1'
    signature = modeling_data_signature()
    if (
        not refresh
        and MODELING_CACHE['payload'] is not None
        and MODELING_CACHE['signature'] == signature
        and MODELING_CACHE['built_at'] is not None
        and time.time() - MODELING_CACHE['built_at'] < CACHE_TTL
    ):
        return MODELING_CACHE['payload']
    if not refresh and MODELING_CACHE_FILE.exists():
        try:
            cached = json.loads(MODELING_CACHE_FILE.read_text(encoding='utf-8'))
            if cached.get('signature') == signature and cached.get('payload') is not None:
                MODELING_CACHE.update({
                    'signature': signature,
                    'built_at': time.time(),
                    'payload': cached['payload'],
                })
                return Response(
                    json.dumps(cached['payload']),
                    mimetype='application/json',
                )
        except (OSError, json.JSONDecodeError):
            pass

    try:
        from sklearn.base import clone
        from sklearn.cluster import KMeans
        from sklearn.decomposition import PCA
        from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor, RandomForestClassifier, RandomForestRegressor
        from sklearn.impute import SimpleImputer
        from sklearn.exceptions import ConvergenceWarning
        from sklearn.linear_model import ElasticNet, Lasso, LogisticRegression, Ridge
        from sklearn.metrics import accuracy_score, mean_absolute_error, mean_squared_error, silhouette_score
        from sklearn.neural_network import MLPClassifier, MLPRegressor
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:
        raise RuntimeError('scikit-learn is required. Run pip install -r requirements.txt into the project environment.') from exc
    warnings.filterwarnings('ignore', category=ConvergenceWarning)

    csv_files = {
        'cot_wti_brent_merged.csv': 'Tuesday COT positioning and Brent/WTI Tuesday closes',
        'cftc_positioning.csv': 'CFTC energy positioning plus USO/UNG ETF volume pressure',
        'petroleum_inventories.csv': 'Weekly EIA petroleum inventories',
        'days_of_supply.csv': 'Weekly inventory days of supply',
        'refinery_runs.csv': 'Monthly refinery crude inputs',
        'crack_spreads.csv': 'Daily crack spreads and Brent-WTI spread',
        'market_prices.csv': 'Daily non-oil market prices and commodities',
        'weekly_balance.csv': 'Weekly EIA balance by product',
        'treasury_yields.csv': 'Daily Treasury yields, fed funds, and curve spreads',
        'credit_spreads.csv': 'Daily credit and Treasury ETF proxies',
        'dxy.csv': 'Daily US Dollar Index',
        'natgas_inventories.csv': 'Weekly natural gas storage',
        'crude_production.csv': 'Monthly crude production and supply adjustment',
        'cpi.csv': 'Monthly CPI component indexes and YoY changes',
        'ppi.csv': 'Monthly PPI component indexes and YoY changes',
        'consumer_sentiment.csv': 'Monthly consumer sentiment indicators',
        'ism_pmi.csv': 'Monthly manufacturing indicators',
        'jobless_claims.csv': 'Weekly labor stress indicators',
        'housing.csv': 'Monthly housing and mortgage indicators',
        'retail_sales.csv': 'Monthly retail sales indicators',
        'transportation.csv': 'Monthly transportation activity',
        'treasury_withholding.csv': 'Daily Treasury withholding cash-flow proxy',
    }

    cot = prepare_date_frame(read_dataframe('cot_wti_brent_merged.csv'))
    base = cot.rename(columns={
        'brent_ww': 'y_brent_ww',
        'brent_close': 'brent_tuesday_close',
        'wti_close': 'wti_tuesday_close',
        'wti_ww': 'wti_tuesday_ww',
    })[
        [
            'date', 'y_brent_ww', 'brent_tuesday_close', 'wti_tuesday_close',
            'wti_tuesday_ww', 'WTI_CME_mm_net', 'BRENT_ICE_mm_net',
            'COMBINED_mm_net', 'COMBINED_mm_net_ww'
        ]
    ].dropna(subset=['date', 'y_brent_ww']).sort_values('date')
    base['target_brent_ww_fwd_1'] = base['y_brent_ww'].shift(-1)
    base['target_date'] = base['date'].shift(-1)

    cot_feature_cols = ['WTI_CME_mm_net', 'BRENT_ICE_mm_net', 'COMBINED_mm_net', 'COMBINED_mm_net_ww']
    cot_features = add_derived_feature_block(base[['date'] + cot_feature_cols], 'date', cot_feature_cols, 'positioning')
    base = base.merge(cot_features, on='date', how='left')

    crack = prepare_date_frame(read_dataframe('crack_spreads.csv'))
    spread_cols = ['gasoline_crack', 'ho_crack', 'crack_321', 'brent_wti_spread']
    base = add_asof_feature_block(base, add_derived_feature_block(crack, 'date', spread_cols, 'spread'), 'spread')

    prices = prepare_date_frame(read_dataframe('market_prices.csv'))
    market_cols = ['nat_gas', 'sp500', 'gold', 'silver', 'copper']
    base = add_asof_feature_block(base, add_derived_feature_block(prices, 'date', market_cols, 'market'), 'market')

    treasury = prepare_date_frame(read_dataframe('treasury_yields.csv'))
    treasury['short_rate_pressure'] = treasury['yield_3mo'] - treasury['fed_funds']
    treasury['curve_belly_2s5s'] = treasury['yield_5yr'] - treasury['yield_2yr']
    treasury['curve_long_5s30s'] = treasury['yield_30yr'] - treasury['yield_5yr']
    treasury_cols = [
        'yield_2yr', 'yield_10yr', 'yield_30yr', 'yield_3mo', 'yield_1yr',
        'yield_5yr', 'fed_funds', 'spread_2s10s', 'spread_3m10y',
        'short_rate_pressure', 'curve_belly_2s5s', 'curve_long_5s30s'
    ]
    base = add_asof_feature_block(base, add_derived_feature_block(treasury, 'date', treasury_cols, 'rates'), 'rates')

    dxy = prepare_date_frame(read_dataframe('dxy.csv'))
    dxy['dxy_ma_gap_50'] = dxy['dxy'] - dxy['dxy_ma50']
    dxy['dxy_ma_gap_200'] = dxy['dxy'] - dxy['dxy_ma200']
    base = add_asof_feature_block(base, add_derived_feature_block(dxy, 'date', ['dxy', 'dxy_ma_gap_50', 'dxy_ma_gap_200'], 'dxy'), 'dxy')

    credit = prepare_date_frame(read_dataframe('credit_spreads.csv'))
    credit['hyg_lqd_ratio_chg_5'] = credit['hyg_lqd_ratio'].diff(5)
    base = add_asof_feature_block(base, add_derived_feature_block(credit, 'date', ['hyg', 'lqd', 'tlt', 'shy', 'jnk', 'hyg_lqd_ratio', 'hyg_lqd_ratio_chg_5'], 'credit'), 'credit')

    inventories = prepare_date_frame(read_dataframe('petroleum_inventories.csv'))
    inv_cols = [col for col in inventories.select_dtypes(include=[np.number]).columns if col not in {'year', 'month', 'week'}]
    inventories['crude_refined_product_tightness'] = inventories['crude_US'] - inventories['total_gasoline_US'] - inventories['distillate_US']
    inventories['light_products_total'] = inventories['total_gasoline_US'] + inventories['distillate_US'] + inventories['jet_fuel_US']
    inv_cols += ['crude_refined_product_tightness', 'light_products_total']
    base = add_asof_feature_block(base, add_derived_feature_block(inventories, 'date', inv_cols, 'inventory'), 'inventory')

    dos = prepare_date_frame(read_dataframe('days_of_supply.csv'))
    dos_cols = [col for col in dos.select_dtypes(include=[np.number]).columns if col not in {'year', 'week'}]
    if {'crude_US_days', 'gasoline_US_days', 'distillate_US_days'}.issubset(dos.columns):
        dos['aggregate_days_supply'] = dos[['crude_US_days', 'gasoline_US_days', 'distillate_US_days']].mean(axis=1)
        dos_cols.append('aggregate_days_supply')
    base = add_asof_feature_block(base, add_derived_feature_block(dos, 'date', dos_cols, 'dos'), 'dos')

    refinery = prepare_date_frame(read_dataframe('refinery_runs.csv'))
    base = add_asof_feature_block(base, add_derived_feature_block(refinery, 'date', ['crude_input'], 'refinery'), 'refinery')

    balance = read_dataframe('weekly_balance.csv')
    balance['date'] = pd.to_datetime(balance['period'], errors='coerce')
    balance_wide = balance.pivot_table(
        index='date',
        columns='product',
        values=['production', 'imports', 'exports', 'refinery_input', 'refinery_output', 'product_supplied', 'spr_stocks', 'spr_change'],
        aggfunc='last',
    )
    balance_wide.columns = [f'{metric}_{product}' for metric, product in balance_wide.columns]
    balance_wide = balance_wide.reset_index().sort_values('date')
    bal_cols = [col for col in balance_wide.columns if col != 'date']
    if {'imports_crude_US', 'exports_crude_US', 'refinery_input_crude_US'}.issubset(balance_wide.columns):
        balance_wide['crude_balance_net_supply'] = balance_wide['imports_crude_US'] - balance_wide['exports_crude_US'] - balance_wide['refinery_input_crude_US']
        bal_cols.append('crude_balance_net_supply')
    base = add_asof_feature_block(base, add_derived_feature_block(balance_wide, 'date', bal_cols, 'balance'), 'balance')

    cftc = prepare_date_frame(read_dataframe('cftc_positioning.csv'))
    cftc_position = cftc.dropna(subset=['wti_oil_mm_net']).copy()
    cftc_cols = [
        'wti_oil_mm_long', 'wti_oil_mm_short', 'wti_oil_mm_net', 'wti_oil_mm_net_pct',
        'wti_oil_oi', 'nat_gas_mm_long', 'nat_gas_mm_short', 'nat_gas_mm_net',
        'nat_gas_mm_net_pct', 'nat_gas_oi', 'rbob_gas_mm_long', 'rbob_gas_mm_short',
        'rbob_gas_mm_net', 'rbob_gas_mm_net_pct', 'rbob_gas_oi'
    ]
    base = add_asof_feature_block(base, add_derived_feature_block(cftc_position, 'date', cftc_cols, 'cftc'), 'cftc')

    etf = cftc[['date', 'uso_volume', 'ung_volume']].copy()
    for prefix in ['uso', 'ung']:
        etf[f'{prefix}_volume_ma50'] = etf[f'{prefix}_volume'].rolling(50, min_periods=25).mean()
        etf[f'{prefix}_volume_pressure'] = (etf[f'{prefix}_volume'] / etf[f'{prefix}_volume_ma50']) - 1
    etf = etf.set_index('date')
    weekly_etf = pd.DataFrame(index=base['date'])
    for cot_date in base['date']:
        window = etf.loc[(etf.index <= cot_date) & (etf.index >= cot_date - pd.Timedelta(days=6))]
        for col in ['uso_volume_pressure', 'ung_volume_pressure']:
            weekly_etf.loc[cot_date, f'etf_{col}'] = window[col].mean() if not window.empty else np.nan
    weekly_etf = weekly_etf.reset_index().rename(columns={'index': 'date'})
    base = base.merge(weekly_etf, on='date', how='left')

    natgas_inv = prepare_date_frame(read_dataframe('natgas_inventories.csv'))
    base = add_asof_feature_block(base, add_derived_feature_block(natgas_inv, 'date', ['storage_bcf', 'storage_change'], 'natgas_inventory'), 'natgas_inventory')

    crude_prod = prepare_date_frame(read_dataframe('crude_production.csv'))
    crude_prod_cols = ['production_kbd', 'adjustment_kbd', 'production_mmbpd', 'adjustment_mmbpd', 'production_smooth', 'adjustment_smooth']
    base = add_asof_feature_block(base, add_derived_feature_block(crude_prod, 'date', crude_prod_cols, 'crude_production'), 'crude_production')

    monthly_sources = [
        ('cpi.csv', 'cpi', ['cpi_core_yoy', 'cpi_food_yoy', 'cpi_shelter_yoy', 'cpi_medical_yoy', 'cpi_transportation_yoy']),
        ('ppi.csv', 'ppi', ['ppi_final_demand_yoy', 'ppi_core_yoy', 'ppi_energy_yoy']),
        ('consumer_sentiment.csv', 'sentiment', ['umich_sentiment', 'umich_expectations', 'conf_board']),
        ('ism_pmi.csv', 'manufacturing', ['mfg_employment', 'industrial_prod_mfg', 'capacity_util_mfg', 'durable_goods_orders', 'new_orders', 'industrial_prod']),
        ('housing.csv', 'housing', ['housing_starts', 'building_permits', 'new_home_sales', 'case_shiller_yoy', 'mortgage_rate_30yr', 'mortgage_rate_15yr', 'existing_home_sales']),
        ('retail_sales.csv', 'retail', ['retail_total_mom', 'retail_total_yoy', 'retail_ecommerce', 'retail_building_materials', 'retail_general_merch']),
        ('transportation.csv', 'transport', ['air_passengers', 'rail_passengers', 'vmt']),
        ('employment.csv', 'employment', ['us_total', 'us_private', 'california', 'texas' if False else 'florida']),
        ('jolts.csv', 'jolts', ['job_openings', 'layoffs', 'separations', 'openings_rate', 'hires_rate', 'quits_rate']),
    ]
    for filename, prefix, cols in monthly_sources:
        frame = prepare_date_frame(read_dataframe(filename))
        usable_cols = [col for col in cols if col in frame.columns]
        if usable_cols:
            base = add_asof_feature_block(base, add_derived_feature_block(frame, 'date', usable_cols, prefix), prefix)

    claims = prepare_date_frame(read_dataframe('jobless_claims.csv'))
    claims_cols = ['initial_claims', 'continued_claims', 'insured_unemployment', 'initial_claims_4wma']
    base = add_asof_feature_block(base, add_derived_feature_block(claims, 'date', claims_cols, 'labor'), 'labor')

    withholding = prepare_date_frame(read_dataframe('treasury_withholding.csv'))
    withholding_cols = ['daily_withholding', 'mtd_withholding', 'fytd_withholding', 'daily_7d_avg', 'daily_30d_avg']
    base = add_asof_feature_block(base, add_derived_feature_block(withholding, 'date', withholding_cols, 'withholding'), 'withholding')

    # Cross-family composites after all as-of joins.
    base['composite_inventory_tightness'] = (
        -base.get('inventory_crude_US_z_26', 0)
        -base.get('inventory_total_gasoline_US_z_26', 0)
        -base.get('inventory_distillate_US_z_26', 0)
    )
    base['composite_positioning_crowding'] = base.get('positioning_COMBINED_mm_net_z_26', np.nan)
    base['composite_macro_risk'] = (
        base.get('dxy_dxy_z_26', 0)
        -base.get('credit_hyg_lqd_ratio_z_26', 0)
        +base.get('labor_initial_claims_4wma_z_26', 0)
    )
    base['composite_rates_pressure'] = (
        base.get('rates_yield_2yr_z_26', 0)
        +base.get('rates_yield_10yr_z_26', 0)
        -base.get('rates_spread_2s10s_z_26', 0)
    )
    base['composite_refining_margin'] = (
        base.get('spread_crack_321_z_26', 0)
        +base.get('spread_gasoline_crack_z_26', 0)
        +base.get('spread_ho_crack_z_26', 0)
    ) / 3

    excluded_predictors = {
        'y_brent_ww', 'target_brent_ww_fwd_1', 'brent_tuesday_close', 'wti_tuesday_close',
        'wti_tuesday_ww', 'target_date',
    }
    forbidden_tokens = [
        'brent_close', 'wti_close', 'brent_tuesday_return', 'crude_oil',
        'uso_price', 'ung_price', 'market_wti', 'market_brent',
    ]
    source_date_cols = [col for col in base.columns if col.endswith('_source_date')]
    candidate_features = [
        col for col in base.select_dtypes(include=[np.number]).columns
        if col not in excluded_predictors
        and col not in source_date_cols
        and not any(token in col.lower() for token in forbidden_tokens)
        and base[col].notna().sum() >= 12
        and base[col].nunique(dropna=True) > 1
    ]
    missing = base[candidate_features].isna().mean()
    feature_cols = [col for col in candidate_features if missing[col] <= 0.92]

    def feature_group(name):
        mapping = [
            ('rates_', 'Rates'), ('inventory_', 'Petroleum Inventories'), ('dos_', 'Days Supply'),
            ('balance_', 'Weekly Balance'), ('positioning_', 'COT Positioning'), ('cftc_', 'CFTC Positioning'),
            ('spread_', 'Cracks/Spreads'), ('etf_', 'ETF Volume'), ('dxy_', 'Dollar'),
            ('credit_', 'Credit'), ('market_', 'Non-Oil Markets'), ('natgas_', 'NatGas'),
            ('crude_production_', 'Production'), ('cpi_', 'Inflation'), ('ppi_', 'Inflation'),
            ('sentiment_', 'Sentiment'), ('manufacturing_', 'Manufacturing'), ('housing_', 'Housing'),
            ('retail_', 'Retail'), ('transport_', 'Transportation'), ('labor_', 'Labor'),
            ('withholding_', 'Treasury Withholding'), ('composite_', 'Composites')
        ]
        for prefix, group in mapping:
            if name.startswith(prefix):
                return group
        return 'Other'

    train_df = base.dropna(subset=['target_brent_ww_fwd_1']).copy()
    y = train_df['target_brent_ww_fwd_1'].astype(float)
    latest_row = base.tail(1)

    def regression_stats(x_values, y_values):
        pair = pd.DataFrame({'x': x_values, 'y': y_values}).replace([np.inf, -np.inf], np.nan).dropna()
        n = len(pair)
        if n < 8 or pair['x'].nunique() < 2:
            return {'n': n, 'corr': None, 'beta': None, 'r2': None}
        corr = pair['x'].corr(pair['y'])
        beta = pair['x'].cov(pair['y']) / pair['x'].var()
        return {'n': int(n), 'corr': to_float(corr), 'beta': to_float(beta), 'r2': to_float(corr * corr)}

    relationships = []
    for col in feature_cols:
        lag_stats = []
        for lag in range(0, 5):
            stats = regression_stats(train_df[col].shift(lag), y)
            lag_stats.append({'lag_weeks': lag, **stats})
        valid = [item for item in lag_stats if item['corr'] is not None]
        if not valid:
            continue
        best = max(valid, key=lambda item: abs(item['corr']))
        relationships.append({
            'feature': col,
            'group': feature_group(col),
            'best_lag_weeks': best['lag_weeks'],
            'corr': best['corr'],
            'r2': best['r2'],
            'beta': best['beta'],
            'n': best['n'],
            'latest': to_float(latest_row[col].iloc[0]) if col in latest_row else None,
            'lag0_corr': lag_stats[0]['corr'],
            'lag1_corr': lag_stats[1]['corr'],
            'lag2_corr': lag_stats[2]['corr'],
            'lag3_corr': lag_stats[3]['corr'],
            'lag4_corr': lag_stats[4]['corr'],
        })
    relationships = sorted(relationships, key=lambda item: abs(item['corr'] or 0), reverse=True)[:60]

    forced_features = [
        'composite_inventory_tightness', 'composite_positioning_crowding',
        'composite_macro_risk', 'composite_rates_pressure', 'composite_refining_margin',
        'etf_uso_volume_pressure', 'etf_ung_volume_pressure',
    ]
    selected_features = []
    for col in forced_features + [row['feature'] for row in relationships]:
        if col in feature_cols and col not in selected_features:
            selected_features.append(col)
    feature_cols = selected_features[:70]
    X = train_df[feature_cols].replace([np.inf, -np.inf], np.nan)
    latest_X = latest_row[feature_cols].replace([np.inf, -np.inf], np.nan)

    reg_models = {
        'Zero Baseline': None,
        'Trailing Mean Baseline': None,
        'Ridge': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('scaler', StandardScaler()), ('model', Ridge(alpha=5.0))]),
        'Lasso': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('scaler', StandardScaler()), ('model', Lasso(alpha=0.04, max_iter=5000))]),
        'ElasticNet': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('scaler', StandardScaler()), ('model', ElasticNet(alpha=0.05, l1_ratio=0.35, max_iter=5000))]),
        'Random Forest': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('model', RandomForestRegressor(n_estimators=45, max_depth=4, min_samples_leaf=5, random_state=42))]),
        'Gradient Boosting': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('model', GradientBoostingRegressor(n_estimators=35, learning_rate=0.05, max_depth=2, random_state=42))]),
        'MLP Regressor': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('scaler', StandardScaler()), ('model', MLPRegressor(hidden_layer_sizes=(10,), alpha=0.25, max_iter=150, random_state=42, early_stopping=True))]),
    }
    clf_models = {
        'Logistic Regression': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('scaler', StandardScaler()), ('model', LogisticRegression(C=0.4, max_iter=1000))]),
        'Random Forest Classifier': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('model', RandomForestClassifier(n_estimators=45, max_depth=4, min_samples_leaf=5, random_state=42))]),
        'Gradient Boosting Classifier': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('model', GradientBoostingClassifier(n_estimators=35, learning_rate=0.05, max_depth=2, random_state=42))]),
        'MLP Classifier': Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)), ('scaler', StandardScaler()), ('model', MLPClassifier(hidden_layer_sizes=(10,), alpha=0.20, max_iter=150, random_state=42, early_stopping=True))]),
    }

    initial_train = max(80, len(train_df) - 20)
    large_threshold = float(y.abs().quantile(0.75))

    def metric_row(name, actual, pred):
        actual = np.array(actual, dtype=float)
        pred = np.array(pred, dtype=float)
        return {
            'model': name,
            'n': int(len(actual)),
            'mae': to_float(mean_absolute_error(actual, pred)),
            'rmse': to_float(np.sqrt(mean_squared_error(actual, pred))),
            'directional_accuracy': to_float((np.sign(actual) == np.sign(pred)).mean() * 100),
            'large_move_hit_rate': to_float((np.sign(actual[np.abs(actual) >= large_threshold]) == np.sign(pred[np.abs(actual) >= large_threshold])).mean() * 100) if (np.abs(actual) >= large_threshold).any() else None,
            'avg_prediction': to_float(np.mean(pred)),
        }

    leaderboard = []
    wf_predictions = {name: [] for name in reg_models}
    wf_actual = []
    wf_dates = []
    for i in range(initial_train, len(train_df)):
        train_slice = slice(0, i)
        test_slice = slice(i, i + 1)
        actual = y.iloc[i]
        wf_actual.append(float(actual))
        wf_dates.append(train_df['target_date'].iloc[i].strftime('%Y-%m-%d'))
        trailing_mean = y.iloc[:i].tail(13).mean()
        wf_predictions['Zero Baseline'].append(0.0)
        wf_predictions['Trailing Mean Baseline'].append(float(trailing_mean))
        for name, model in reg_models.items():
            if model is None:
                continue
            fitted = clone(model)
            fitted.fit(X.iloc[train_slice], y.iloc[train_slice])
            wf_predictions[name].append(float(fitted.predict(X.iloc[test_slice])[0]))

    for name, preds in wf_predictions.items():
        leaderboard.append(metric_row(name, wf_actual, preds))
    leaderboard = sorted(leaderboard, key=lambda row: row['rmse'] if row['rmse'] is not None else 999)

    classifier_leaderboard = []
    class_actual = []
    class_predictions = {name: [] for name in clf_models}
    class_probs = {name: [] for name in clf_models}
    y_class = (y > 0).astype(int)
    for i in range(initial_train, len(train_df)):
        class_actual.append(int(y_class.iloc[i]))
        for name, model in clf_models.items():
            fitted = clone(model)
            fitted.fit(X.iloc[:i], y_class.iloc[:i])
            class_predictions[name].append(int(fitted.predict(X.iloc[i:i + 1])[0]))
            if hasattr(fitted.named_steps['model'], 'predict_proba'):
                class_probs[name].append(float(fitted.predict_proba(X.iloc[i:i + 1])[0][1]))
            else:
                class_probs[name].append(None)
    for name, preds in class_predictions.items():
        classifier_leaderboard.append({
            'model': name,
            'n': int(len(class_actual)),
            'directional_accuracy': to_float(accuracy_score(class_actual, preds) * 100),
            'positive_prediction_rate': to_float(np.mean(preds) * 100),
            'avg_up_probability': to_float(np.nanmean([p for p in class_probs[name] if p is not None])) if any(p is not None for p in class_probs[name]) else None,
        })
    classifier_leaderboard = sorted(classifier_leaderboard, key=lambda row: row['directional_accuracy'] or 0, reverse=True)

    final_predictions = []
    fitted_models = {}
    for name, model in reg_models.items():
        if model is None:
            pred = 0.0 if name == 'Zero Baseline' else float(y.tail(13).mean())
        else:
            fitted = clone(model)
            fitted.fit(X, y)
            fitted_models[name] = fitted
            pred = float(fitted.predict(latest_X)[0])
        final_predictions.append({'model': name, 'prediction': to_float(pred)})

    classifier_predictions = []
    for name, model in clf_models.items():
        fitted = clone(model)
        fitted.fit(X, y_class)
        pred_class = int(fitted.predict(latest_X)[0])
        probability = float(fitted.predict_proba(latest_X)[0][1]) if hasattr(fitted.named_steps['model'], 'predict_proba') else None
        classifier_predictions.append({'model': name, 'up_probability': to_float(probability), 'direction': 'Up' if pred_class else 'Down'})

    best_model_name = next(row['model'] for row in leaderboard if 'Baseline' not in row['model'])
    best_prediction = next(row['prediction'] for row in final_predictions if row['model'] == best_model_name)
    avg_up_probability = np.nanmean([row['up_probability'] for row in classifier_predictions if row['up_probability'] is not None])

    rf_model = fitted_models.get('Random Forest')
    rf_importance = []
    if rf_model is not None:
        values = rf_model.named_steps['model'].feature_importances_
        rf_importance = sorted([
            {'feature': col, 'group': feature_group(col), 'importance': to_float(val)}
            for col, val in zip(feature_cols, values)
        ], key=lambda row: row['importance'] or 0, reverse=True)[:30]

    ridge_model = fitted_models.get('Ridge')
    linear_importance = []
    linear_contributions = []
    if ridge_model is not None:
        coefs = ridge_model.named_steps['model'].coef_
        linear_importance = sorted([
            {'feature': col, 'group': feature_group(col), 'coefficient': to_float(coef), 'abs_coefficient': to_float(abs(coef))}
            for col, coef in zip(feature_cols, coefs)
        ], key=lambda row: row['abs_coefficient'] or 0, reverse=True)[:30]
        transformed_latest = ridge_model.named_steps['scaler'].transform(
            ridge_model.named_steps['imputer'].transform(latest_X)
        )[0]
        linear_contributions = sorted([
            {
                'feature': col,
                'group': feature_group(col),
                'z_value': to_float(z_val),
                'coefficient': to_float(coef),
                'contribution': to_float(z_val * coef),
            }
            for col, z_val, coef in zip(feature_cols, transformed_latest, coefs)
        ], key=lambda row: abs(row['contribution'] or 0), reverse=True)[:20]

    cluster_frame = base[feature_cols].replace([np.inf, -np.inf], np.nan)
    imputer = SimpleImputer(strategy='median', keep_empty_features=True)
    scaler = StandardScaler()
    cluster_values = scaler.fit_transform(imputer.fit_transform(cluster_frame))
    pca = PCA(n_components=2, random_state=42)
    pca_values = pca.fit_transform(cluster_values)
    silhouette_rows = []
    for k in range(3, 7):
        labels = KMeans(n_clusters=k, random_state=42, n_init=20).fit_predict(cluster_values)
        score = silhouette_score(cluster_values, labels) if len(set(labels)) > 1 else np.nan
        silhouette_rows.append({'k': k, 'silhouette': to_float(score)})
    k4_score = next(row['silhouette'] for row in silhouette_rows if row['k'] == 4)
    best_k_row = max(silhouette_rows, key=lambda row: row['silhouette'] if row['silhouette'] is not None else -999)
    selected_k = best_k_row['k'] if k4_score is None or (best_k_row['silhouette'] or -999) > k4_score + 0.03 else 4
    kmeans = KMeans(n_clusters=selected_k, random_state=42, n_init=30)
    cluster_labels = kmeans.fit_predict(cluster_values)
    current_cluster = int(cluster_labels[-1])
    cluster_points = [
        {
            'date': row['date'].strftime('%Y-%m-%d'),
            'pc1': to_float(pca_values[i, 0]),
            'pc2': to_float(pca_values[i, 1]),
            'cluster': int(cluster_labels[i]),
            'target': to_float(row['target_brent_ww_fwd_1']),
        }
        for i, (_, row) in enumerate(base.iterrows())
    ]
    cluster_stats = []
    for cluster_id in sorted(set(cluster_labels)):
        idx = np.where(cluster_labels[:len(train_df)] == cluster_id)[0]
        cluster_y = train_df.iloc[idx]['target_brent_ww_fwd_1'].dropna() if len(idx) else pd.Series(dtype=float)
        center = kmeans.cluster_centers_[cluster_id]
        top_center_features = sorted([
            {'feature': col, 'group': feature_group(col), 'center_z': to_float(val)}
            for col, val in zip(feature_cols, center)
        ], key=lambda row: abs(row['center_z'] or 0), reverse=True)[:8]
        cluster_stats.append({
            'cluster': int(cluster_id),
            'n': int(cluster_y.count()),
            'avg_target': to_float(cluster_y.mean()) if len(cluster_y) else None,
            'volatility': to_float(cluster_y.std()) if len(cluster_y) > 1 else None,
            'positive_hit_rate': to_float((cluster_y > 0).mean() * 100) if len(cluster_y) else None,
            'is_current': bool(cluster_id == current_cluster),
            'dominant_features': top_center_features,
        })

    distances = np.linalg.norm(cluster_values[:-1] - cluster_values[-1], axis=1)
    similar_indices = np.argsort(distances)[:10]
    similar_weeks = [
        {
            'date': base.iloc[int(i)]['date'].strftime('%Y-%m-%d'),
            'target_date': base.iloc[int(i)]['target_date'].strftime('%Y-%m-%d') if pd.notna(base.iloc[int(i)]['target_date']) else None,
            'target': to_float(base.iloc[int(i)]['target_brent_ww_fwd_1']),
            'cluster': int(cluster_labels[int(i)]),
            'distance': to_float(distances[int(i)]),
        }
        for i in similar_indices
    ]

    rolling = []
    for item in relationships[:10]:
        col = item['feature']
        corr = train_df[col].rolling(26, min_periods=16).corr(train_df['target_brent_ww_fwd_1'])
        rolling.append({
            'feature': col,
            'group': feature_group(col),
            'dates': train_df['date'].dt.strftime('%Y-%m-%d').tolist(),
            'values': [to_float(value) for value in corr.tolist()],
        })

    group_summary = []
    for group in sorted({feature_group(col) for col in feature_cols}):
        cols = [col for col in feature_cols if feature_group(col) == group]
        latest_vals = latest_row[cols].replace([np.inf, -np.inf], np.nan).iloc[0]
        group_summary.append({
            'group': group,
            'features': len(cols),
            'latest_abs_z_avg': to_float(np.nanmean(np.abs(latest_vals))) if len(cols) else None,
            'top_relationship_corr': max([abs(row['corr']) for row in relationships if row['group'] == group and row['corr'] is not None] or [0]),
        })

    last_rows = base.tail(12).copy()
    last_rows['date'] = last_rows['date'].dt.strftime('%Y-%m-%d')
    last_rows['target_date'] = last_rows['target_date'].dt.strftime('%Y-%m-%d')
    last_display_cols = [
        'date', 'target_date', 'target_brent_ww_fwd_1',
        'positioning_COMBINED_mm_net_ww', 'spread_crack_321', 'spread_brent_wti_spread',
        'rates_yield_10yr', 'rates_spread_2s10s', 'inventory_crude_US_chg_1',
        'composite_inventory_tightness', 'composite_macro_risk', 'etf_uso_volume_pressure'
    ]

    target = train_df['target_brent_ww_fwd_1'].dropna()
    target_summary = {
        'observations': int(target.count()),
        'feature_count': len(feature_cols),
        'start_date': train_df['date'].min().strftime('%Y-%m-%d'),
        'end_date': train_df['date'].max().strftime('%Y-%m-%d'),
        'latest_feature_date': base['date'].max().strftime('%Y-%m-%d'),
        'next_target_date': base['target_date'].dropna().max().strftime('%Y-%m-%d') if base['target_date'].notna().any() else None,
        'mean': to_float(target.mean()),
        'median': to_float(target.median()),
        'std': to_float(target.std()),
        'positive_hit_rate': to_float((target > 0).mean() * 100),
        'large_move_threshold': to_float(large_threshold),
    }

    current_signal = {
        'as_of_date': base['date'].max().strftime('%Y-%m-%d'),
        'target': 'Next COT-week Brent Tuesday-close change',
        'best_regression_model': best_model_name,
        'best_model_prediction': to_float(best_prediction),
        'ensemble_prediction': to_float(np.mean([row['prediction'] for row in final_predictions if 'Baseline' not in row['model']])),
        'up_probability': to_float(avg_up_probability * 100) if np.isfinite(avg_up_probability) else None,
        'direction': 'Up' if (avg_up_probability >= 0.5 if np.isfinite(avg_up_probability) else best_prediction >= 0) else 'Down',
        'confidence': to_float(abs((avg_up_probability - 0.5) * 200)) if np.isfinite(avg_up_probability) else None,
        'current_regime': current_cluster,
        'current_regime_label': f'Regime {current_cluster}',
    }

    feature_docs = [
        {'field': 'target_brent_ww_fwd_1', 'formula': 'Next COT Tuesday Brent close minus current COT Tuesday Brent close', 'source': 'cot_wti_brent_merged.csv', 'alignment': 'Target is shifted one COT week forward'},
        {'field': '*_chg_1 / *_chg_4 / *_chg_13', 'formula': 'Source value minus value 1, 4, or 13 source rows earlier', 'source': 'All feature sources', 'alignment': 'Computed inside source frequency before backward as-of join'},
        {'field': '*_z_13 / *_z_26', 'formula': '(value - trailing mean) / trailing standard deviation over 13 or 26 source rows', 'source': 'All feature sources', 'alignment': 'Computed before as-of join'},
        {'field': '*_pctile_26', 'formula': 'Trailing 26-row percentile rank of latest source value', 'source': 'All feature sources', 'alignment': 'Computed before as-of join'},
        {'field': 'composite_inventory_tightness', 'formula': 'Negative z-score sum of crude, gasoline, and distillate inventories; higher means tighter stocks', 'source': 'petroleum_inventories.csv', 'alignment': 'Latest weekly period on or before COT Tuesday'},
        {'field': 'composite_positioning_crowding', 'formula': '26-row z-score of combined Brent + WTI managed-money net length', 'source': 'cot_wti_brent_merged.csv', 'alignment': 'COT Tuesday'},
        {'field': 'composite_macro_risk', 'formula': 'DXY z-score minus HYG/LQD ratio z-score plus claims 4WMA z-score', 'source': 'dxy, credit_spreads, jobless_claims', 'alignment': 'Backward as-of to COT Tuesday'},
        {'field': 'composite_rates_pressure', 'formula': '2Y yield z-score plus 10Y yield z-score minus 2s10s spread z-score', 'source': 'treasury_yields.csv', 'alignment': 'Backward as-of to COT Tuesday'},
        {'field': 'etf_*_volume_pressure', 'formula': 'Tuesday-ending weekly average of volume / trailing 50-day volume average - 1', 'source': 'cftc_positioning.csv', 'alignment': 'Tuesday-ending six-calendar-day window'},
        {'field': 'model validation', 'formula': 'Expanding-window walk-forward validation over the latest available target weeks', 'source': 'Derived feature matrix', 'alignment': 'Each validation row trains only on earlier rows'},
    ]

    payload = clean_json({
        'schemas': [schema_for_csv(name, desc) for name, desc in csv_files.items()],
        'target_summary': target_summary,
        'current_signal': current_signal,
        'model_leaderboard': leaderboard,
        'classifier_leaderboard': classifier_leaderboard,
        'final_predictions': final_predictions,
        'classifier_predictions': classifier_predictions,
        'relationships': relationships,
        'rolling_correlations': rolling,
        'rf_feature_importance': rf_importance,
        'linear_feature_importance': linear_importance,
        'linear_contributions': linear_contributions,
        'clusters': {
            'selected_k': selected_k,
            'silhouette': silhouette_rows,
            'current_cluster': current_cluster,
            'points': cluster_points,
            'stats': cluster_stats,
            'similar_weeks': similar_weeks,
            'pca_explained_variance': [to_float(v) for v in pca.explained_variance_ratio_],
        },
        'group_summary': group_summary,
        'target_series': [
            {
                'date': row['date'].strftime('%Y-%m-%d'),
                'target_date': row['target_date'].strftime('%Y-%m-%d') if pd.notna(row['target_date']) else None,
                'target_brent_ww_fwd_1': to_float(row['target_brent_ww_fwd_1']),
            }
            for _, row in train_df[['date', 'target_date', 'target_brent_ww_fwd_1']].dropna().iterrows()
        ],
        'walk_forward': {
            'dates': wf_dates,
            'actual': [to_float(v) for v in wf_actual],
            'predictions': {name: [to_float(v) for v in values] for name, values in wf_predictions.items()},
        },
        'last_rows': last_rows[[col for col in last_display_cols if col in last_rows.columns]].to_dict('records'),
        'feature_docs': feature_docs,
        'feature_columns': feature_cols,
        'excluded_predictors': sorted(excluded_predictors),
        'forbidden_tokens': forbidden_tokens,
        'caveats': [
            'Target is next COT-week Brent Tuesday-close change.',
            'Features are joined backward as-of to the COT Tuesday row.',
            'Direct Brent, WTI, crude-oil, and oil ETF price/return predictors are excluded.',
            'Sample size is small for machine learning; model outputs are exploratory, not trading signals.',
        ],
    })
    MODELING_CACHE.update({
        'signature': signature,
        'built_at': time.time(),
        'payload': payload,
    })
    try:
        MODELING_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        MODELING_CACHE_FILE.write_text(
            json.dumps({'signature': signature, 'payload': payload}),
            encoding='utf-8',
        )
    except OSError:
        pass
    return payload


@app.route('/api/energy/modeling/summary')
@safe_endpoint
def get_energy_modeling_summary():
    """Lightweight Brent model signal for embedding. Never blocks on a model build."""
    signature = modeling_data_signature()
    payload = None
    fresh = False
    if MODELING_CACHE['payload'] is not None and MODELING_CACHE['signature'] == signature:
        payload = MODELING_CACHE['payload']
        fresh = True
    elif MODELING_CACHE_FILE.exists():
        try:
            cached = json.loads(MODELING_CACHE_FILE.read_text(encoding='utf-8'))
            if cached.get('payload') is not None:
                payload = cached['payload']
                fresh = cached.get('signature') == signature
                if fresh:
                    MODELING_CACHE.update({'signature': signature, 'built_at': time.time(), 'payload': payload})
        except (OSError, json.JSONDecodeError):
            pass
    elif MODELING_CACHE['payload'] is not None:
        payload = MODELING_CACHE['payload']

    if not fresh and MODELING_BUILD_STATE['status'] != 'building':
        start_modeling_build()

    if payload is None:
        status = MODELING_BUILD_STATE['status']
        return {
            'status': 'building' if status in ('idle', 'building', 'ready') else status,
            'build': dict(MODELING_BUILD_STATE),
        }

    signal = payload.get('current_signal') or {}
    summary = payload.get('target_summary') or {}
    leaderboard = payload.get('model_leaderboard') or []
    best = next((row for row in leaderboard if 'Baseline' not in str(row.get('model', ''))), None)
    return {
        'status': 'ready' if fresh else 'stale',
        'build': dict(MODELING_BUILD_STATE),
        'current_signal': signal,
        'next_target_date': summary.get('next_target_date'),
        'observations': summary.get('observations'),
        'best_model': best,
    }


@app.route('/api/treasury/withholding')
@safe_endpoint
def get_treasury_withholding():
    """Treasury tax withholding daily data"""
    return read_csv('treasury_withholding.csv')


@app.route('/api/treasury/withholding/detail')
@safe_endpoint
def get_treasury_withholding_detail():
    """Treasury withholding detailed breakdown by category"""
    return read_csv('treasury_withholding_detail.csv')


@app.route('/api/transportation')
@safe_endpoint
def get_transportation():
    """Air passenger miles and VMT from FRED"""
    return read_csv('transportation.csv')


@app.route('/api/refinery')
@safe_endpoint
def get_refinery():
    """US refinery runs and utilization from EIA PSM"""
    return read_csv('refinery_runs.csv')


@app.route('/api/employment')
@safe_endpoint
def get_employment():
    """Employment by state from FRED/BLS"""
    return read_csv('employment.csv')


@app.route('/api/jolts')
@safe_endpoint
def get_jolts():
    """JOLTS data - job openings, hires, quits"""
    return read_csv('jolts.csv')


@app.route('/api/cpi')
@safe_endpoint
def get_cpi():
    """CPI data - all items, core, components"""
    return read_csv('cpi.csv')


@app.route('/api/ppi')
@safe_endpoint
def get_ppi():
    """PPI data - final demand, components"""
    return read_csv('ppi.csv')


@app.route('/api/market-prices')
@safe_endpoint
def get_market_prices():
    """Daily market prices from Yahoo Finance"""
    return read_csv('market_prices.csv')


@app.route('/api/oil-stocks')
@safe_endpoint
def get_oil_stocks():
    """WTI and oil & gas company stock prices (normalized)"""
    return read_csv('oil_stocks.csv')


@app.route('/api/treasury-yields')
@safe_endpoint
def get_treasury_yields():
    """Treasury yields and yield curve data"""
    return read_csv('treasury_yields.csv')


@app.route('/api/jobless-claims')
@safe_endpoint
def get_jobless_claims():
    """Initial and continuing jobless claims"""
    return read_csv('jobless_claims.csv')


@app.route('/api/ism-pmi')
@safe_endpoint
def get_ism_pmi():
    """ISM Manufacturing and Services PMI"""
    return read_csv('ism_pmi.csv')


@app.route('/api/housing')
@safe_endpoint
def get_housing():
    """Housing market data"""
    return read_csv('housing.csv')


@app.route('/api/retail-sales')
@safe_endpoint
def get_retail_sales():
    """Retail sales data"""
    return read_csv('retail_sales.csv')


@app.route('/api/consumer-sentiment')
@safe_endpoint
def get_consumer_sentiment():
    """Consumer sentiment data"""
    return read_csv('consumer_sentiment.csv')


@app.route('/api/credit-spreads')
@safe_endpoint
def get_credit_spreads():
    """Credit spread ETF data"""
    return read_csv('credit_spreads.csv')


@app.route('/api/dxy')
@safe_endpoint
def get_dxy():
    """US Dollar Index data"""
    return read_csv('dxy.csv')


@app.route('/api/natgas-inventories')
@safe_endpoint
def get_natgas_inventories():
    """Natural gas storage inventories"""
    return read_csv('natgas_inventories.csv')


@app.route('/api/baltic-dry')
@safe_endpoint
def get_baltic_dry():
    """Baltic Dry Index (shipping ETF proxy)"""
    return read_csv('baltic_dry.csv')


@app.route('/api/ev/monthly')
@safe_endpoint
def get_ev_monthly():
    """Monthly EV fleet and sales-share data."""
    return read_csv('ev_monthly_fleet.csv')


@app.route('/api/ev/annual')
@safe_endpoint
def get_ev_annual():
    """Annual EV fleet and sales data."""
    return read_csv('ev_annual_fleet.csv')


@app.route('/api/ev/policies')
@safe_endpoint
def get_ev_policies():
    """Policy markers used by the EV fleet dashboard."""
    return read_csv('ev_policy_events.csv')


@app.route('/api/status')
def status():
    """Check data status"""
    try:
        meta_file = DATA_DIR / 'metadata.csv'
        if meta_file.exists():
            meta = pd.read_csv(meta_file).to_dict('records')[0]
        else:
            meta = {'last_updated': 'Never', 'files': []}

        files = [
            'gdp_components', 'gdp_contributions', 'trade_detail', 'trade_categories',
            'detailed_trade', 'services_trade', 'commodities', 'petroleum_inventories',
            'supply_demand', 'weekly_balance', 'treasury_withholding', 'transportation',
            'refinery_runs', 'employment', 'jolts', 'cpi', 'ppi', 'market_prices', 'oil_stocks',
            'treasury_yields', 'jobless_claims', 'ism_pmi', 'housing', 'retail_sales',
            'consumer_sentiment', 'credit_spreads', 'dxy', 'natgas_inventories', 'baltic_dry',
            'days_of_supply', 'crack_spreads', 'rig_count', 'cftc_positioning',
            'cot_wti_brent_merged', 'total_inv_eia_fair_value', 'crude_production',
            'upstream_activity',
            'ev_carsales_monthly_raw', 'ev_brent_monthly', 'ev_monthly_fleet',
            'ev_annual_fleet', 'ev_policy_events'
        ]
        return jsonify({
            'last_updated': meta.get('last_updated', 'Never'),
            'cache_ttl': CACHE_TTL,
            'data_dir': str(DATA_DIR),
            'bundled_data_dir': str(BUNDLED_DATA_DIR),
            'uses_external_data_dir': str(DATA_DIR.resolve()) != str(BUNDLED_DATA_DIR.resolve()),
            'bundled_data_sync': DATA_SYNC_STATE,
            'files_exist': {f: (DATA_DIR / f'{f}.csv').exists() for f in files},
            'spr_files_exist': {
                f: (DATA_DIR / f'{f}.csv').exists()
                for f in [
                    'spr_weekly_inventory', 'spr_release_events', 'spr_site_quality',
                    'spr_site_info', 'spr_monthly_summary', 'spr_release_quality',
                    'spr_release_buyers', 'spr_news', 'spr_release_summary',
                    'spr_buyer_award_delivery_windows', 'spr_buyer_award_totals',
                    'spr_buyer_award_window_rows'
                ]
            },
            'spr_reports_exist': {
                f: (SPR_REPORT_DIR / f).exists()
                for f in [
                    'spr_release_report_latest.html',
                    'spr_buyer_awards_report_latest.html'
                ]
            },
            'refresh': get_refresh_snapshot(),
            'spr_refresh': get_spr_refresh_snapshot()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/refresh/status')
def refresh_status():
    """Check the currently running or most recent refresh."""
    return jsonify(get_refresh_snapshot())


@app.route('/api/spr/refresh/status')
def spr_refresh_status():
    """Check the currently running or most recent SPR refresh."""
    return jsonify(get_spr_refresh_snapshot())


@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """Run data_fetcher.py to refresh all data (async)"""
    global REFRESH_PROCESS

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        script_path = APP_DIR / 'data_fetcher.py'

        with REFRESH_LOCK:
            current = refresh_snapshot_locked()
            if current['is_running']:
                return jsonify({
                    'success': False,
                    'message': 'A data refresh is already running.',
                    'refresh': current
                }), 409

            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'

            with open(REFRESH_LOG, 'a', encoding='utf-8') as log_file:
                log_file.write(f"\n===== Refresh started {now_iso()} =====\n")
                log_file.flush()
                REFRESH_PROCESS = subprocess.Popen(
                    [sys.executable, str(script_path)],
                    cwd=str(APP_DIR),
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    env=env
                )

            REFRESH_STATE.update({
                'status': 'running',
                'started_at': now_iso(),
                'finished_at': None,
                'returncode': None,
                'message': 'Data refresh is running.'
            })
            read_csv_cached.cache_clear()
            snapshot = refresh_snapshot_locked()

        return jsonify({
            'success': True,
            'message': 'Data refresh started.',
            'refresh': snapshot
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/spr/refresh', methods=['POST'])
def refresh_spr_data():
    """Run spr_release_fetcher.py to refresh only SPR release data (async)."""
    global SPR_REFRESH_PROCESS

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        script_path = APP_DIR / 'spr_release_fetcher.py'

        with SPR_REFRESH_LOCK:
            current = spr_refresh_snapshot_locked()
            if current['is_running']:
                return jsonify({
                    'success': False,
                    'message': 'An SPR data refresh is already running.',
                    'refresh': current
                }), 409

            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'

            with open(SPR_REFRESH_LOG, 'a', encoding='utf-8') as log_file:
                log_file.write(f"\n===== SPR refresh started {now_iso()} =====\n")
                log_file.flush()
                SPR_REFRESH_PROCESS = subprocess.Popen(
                    [sys.executable, str(script_path)],
                    cwd=str(APP_DIR),
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    env=env
                )

            SPR_REFRESH_STATE.update({
                'status': 'running',
                'started_at': now_iso(),
                'finished_at': None,
                'returncode': None,
                'message': 'SPR data refresh is running.'
            })
            read_csv_cached.cache_clear()
            snapshot = spr_refresh_snapshot_locked()

        return jsonify({
            'success': True,
            'message': 'SPR data refresh started.',
            'refresh': snapshot
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    """Manually clear the data cache"""
    try:
        read_csv_cached.cache_clear()
        return jsonify({'success': True, 'message': 'Cache cleared'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


def warm_modeling_cache_on_boot():
    """Prebuild the Brent modeling cache so the first dashboard visit doesn't block.

    If the disk cache already matches the current data signature it is loaded
    directly; otherwise a background build starts. Disable with
    MODELING_WARM_ON_BOOT=0 (e.g. in test environments).
    """
    if os.environ.get('MODELING_WARM_ON_BOOT', '1').lower() in ('0', 'false', 'no'):
        return
    try:
        signature = modeling_data_signature()
        if MODELING_CACHE_FILE.exists():
            cached = json.loads(MODELING_CACHE_FILE.read_text(encoding='utf-8'))
            if cached.get('signature') == signature and cached.get('payload') is not None:
                MODELING_CACHE.update({'signature': signature, 'built_at': time.time(), 'payload': cached['payload']})
                MODELING_BUILD_STATE.update({'status': 'ready'})
                return
    except (OSError, json.JSONDecodeError):
        pass
    start_modeling_build()


warm_modeling_cache_on_boot()


if __name__ == '__main__':
    print("=" * 50)
    print("Macro Data Tracker")
    print("=" * 50)
    port = int(os.environ.get('PORT', '5003'))
    host = os.environ.get('HOST', '127.0.0.1')
    print(f"URL: http://{host}:{port}")
    print(f"Data dir: {DATA_DIR}")
    print("Run 'python data_fetcher.py' to update data")
    print("=" * 50)

    # Use environment variable for debug mode (default: False in production)
    debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(debug=debug_mode, host=host, port=port)
