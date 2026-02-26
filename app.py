"""
Macro Data Tracker - Dashboard
Reads data from CSV files (run data_fetcher.py to update data)
"""

import os
from datetime import datetime, timedelta
from functools import lru_cache, wraps
from pathlib import Path
import subprocess
import sys
import time

from flask import Flask, render_template, jsonify
import numpy as np
import pandas as pd

app = Flask(__name__)
DATA_DIR = Path(__file__).parent / 'data'

# Cache configuration - cache invalidates every 5 minutes
CACHE_TTL = 300  # seconds


def get_cache_key():
    """Return a cache key that changes every CACHE_TTL seconds"""
    return int(time.time() // CACHE_TTL)


def safe_endpoint(f):
    """Decorator for consistent error handling on API endpoints"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            data = f(*args, **kwargs)
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


@app.route('/')
def index():
    return render_template('index.html')


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


@app.route('/api/cftc-positioning')
@safe_endpoint
def get_cftc_positioning():
    """CFTC Commitment of Traders positioning data"""
    return read_csv('cftc_positioning.csv')


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
            'days_of_supply', 'crack_spreads', 'rig_count', 'cftc_positioning', 'crude_production'
        ]
        return jsonify({
            'last_updated': meta.get('last_updated', 'Never'),
            'cache_ttl': CACHE_TTL,
            'files_exist': {f: (DATA_DIR / f'{f}.csv').exists() for f in files}
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """Run data_fetcher.py to refresh all data (async)"""
    try:
        script_path = Path(__file__).parent / 'data_fetcher.py'
        # Run in background - don't wait for completion
        subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        # Clear the cache
        read_csv_cached.cache_clear()
        return jsonify({'success': True, 'message': 'Data refresh started. This takes 2-3 minutes. Reload the page shortly.'})
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


if __name__ == '__main__':
    print("=" * 50)
    print("Macro Data Tracker")
    print("=" * 50)
    print("URL: http://localhost:5003")
    print(f"Data dir: {DATA_DIR}")
    print("Run 'python data_fetcher.py' to update data")
    print("=" * 50)

    # Use environment variable for debug mode (default: False in production)
    debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(debug=debug_mode, port=5003)
