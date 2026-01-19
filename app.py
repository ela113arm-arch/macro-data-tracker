"""
Macro Data Tracker - Dashboard
Reads data from CSV files (run data_fetcher.py to update data)
"""

from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import sys

from flask import Flask, render_template, jsonify
import numpy as np
import pandas as pd

app = Flask(__name__)
DATA_DIR = Path(__file__).parent / 'data'


def read_csv(filename):
    """Read CSV file and return as dict"""
    filepath = DATA_DIR / filename
    if not filepath.exists():
        return []
    df = pd.read_csv(filepath)
    df = df.replace({np.nan: None})
    return df.to_dict('records')


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/gdp/components')
def get_gdp_components():
    """GDP and components data"""
    data = read_csv('gdp_components.csv')
    return jsonify(data)


@app.route('/api/gdp-contributions')
def get_gdp_contributions():
    """GDP growth contributions"""
    data = read_csv('gdp_contributions.csv')
    return jsonify(data)


@app.route('/api/trade/detail')
def get_trade_detail():
    """Trade breakdown (goods vs services)"""
    data = read_csv('trade_detail.csv')
    return jsonify(data)


@app.route('/api/trade/categories')
def get_trade_categories():
    """All trade categories (imports, exports, balances)"""
    data = read_csv('trade_categories.csv')
    return jsonify(data)


@app.route('/api/trade/detailed')
def get_detailed_trade():
    """Detailed trade categories from BEA ITA (pharma, metals, semiconductors)"""
    data = read_csv('detailed_trade.csv')
    return jsonify(data)


@app.route('/api/trade/services')
def get_services_trade():
    """Services trade breakdown"""
    data = read_csv('services_trade.csv')
    return jsonify(data)


@app.route('/api/commodities')
def get_commodities():
    """Commodity prices"""
    data = read_csv('commodities.csv')
    return jsonify(data)


@app.route('/api/petroleum/inventories')
def get_petroleum_inventories():
    """Weekly petroleum inventories by product and PADD (last 5 years)"""
    data = read_csv('petroleum_inventories.csv')
    if data:
        cutoff = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
        data = [row for row in data if row.get('date', '') >= cutoff]
    return jsonify(data)


@app.route('/api/petroleum/metadata')
def get_petroleum_metadata():
    """Metadata for petroleum inventory series"""
    data = read_csv('petroleum_inventories_metadata.csv')
    return jsonify(data)


@app.route('/api/petroleum/supply-demand')
def get_supply_demand():
    """Monthly supply/demand data from EIA PSM"""
    data = read_csv('supply_demand.csv')
    return jsonify(data)


@app.route('/api/petroleum/weekly-balance')
def get_weekly_balance():
    """Weekly supply/demand balance from EIA WPSR for all products"""
    data = read_csv('weekly_balance.csv')
    return jsonify(data)


@app.route('/api/treasury/withholding')
def get_treasury_withholding():
    """Treasury tax withholding daily data"""
    data = read_csv('treasury_withholding.csv')
    return jsonify(data)


@app.route('/api/treasury/withholding/detail')
def get_treasury_withholding_detail():
    """Treasury withholding detailed breakdown by category"""
    data = read_csv('treasury_withholding_detail.csv')
    return jsonify(data)


@app.route('/api/transportation')
def get_transportation():
    """Air passenger miles and VMT from FRED"""
    data = read_csv('transportation.csv')
    return jsonify(data)


@app.route('/api/refinery')
def get_refinery():
    """US refinery runs and utilization from EIA PSM"""
    data = read_csv('refinery_runs.csv')
    return jsonify(data)


@app.route('/api/employment')
def get_employment():
    """Employment by state from FRED/BLS"""
    data = read_csv('employment.csv')
    return jsonify(data)


@app.route('/api/jolts')
def get_jolts():
    """JOLTS data - job openings, hires, quits"""
    data = read_csv('jolts.csv')
    return jsonify(data)


@app.route('/api/cpi')
def get_cpi():
    """CPI data - all items, core, components"""
    data = read_csv('cpi.csv')
    return jsonify(data)


@app.route('/api/ppi')
def get_ppi():
    """PPI data - final demand, components"""
    data = read_csv('ppi.csv')
    return jsonify(data)


@app.route('/api/market-prices')
def get_market_prices():
    """Daily market prices from Yahoo Finance"""
    data = read_csv('market_prices.csv')
    return jsonify(data)


@app.route('/api/oil-stocks')
def get_oil_stocks():
    """WTI and oil & gas company stock prices (normalized)"""
    data = read_csv('oil_stocks.csv')
    return jsonify(data)


@app.route('/api/treasury-yields')
def get_treasury_yields():
    """Treasury yields and yield curve data"""
    data = read_csv('treasury_yields.csv')
    return jsonify(data)


@app.route('/api/jobless-claims')
def get_jobless_claims():
    """Initial and continuing jobless claims"""
    data = read_csv('jobless_claims.csv')
    return jsonify(data)


@app.route('/api/ism-pmi')
def get_ism_pmi():
    """ISM Manufacturing and Services PMI"""
    data = read_csv('ism_pmi.csv')
    return jsonify(data)


@app.route('/api/housing')
def get_housing():
    """Housing market data"""
    data = read_csv('housing.csv')
    return jsonify(data)


@app.route('/api/retail-sales')
def get_retail_sales():
    """Retail sales data"""
    data = read_csv('retail_sales.csv')
    return jsonify(data)


@app.route('/api/consumer-sentiment')
def get_consumer_sentiment():
    """Consumer sentiment data"""
    data = read_csv('consumer_sentiment.csv')
    return jsonify(data)


@app.route('/api/credit-spreads')
def get_credit_spreads():
    """Credit spread ETF data"""
    data = read_csv('credit_spreads.csv')
    return jsonify(data)


@app.route('/api/dxy')
def get_dxy():
    """US Dollar Index data"""
    data = read_csv('dxy.csv')
    return jsonify(data)


@app.route('/api/natgas-inventories')
def get_natgas_inventories():
    """Natural gas storage inventories"""
    data = read_csv('natgas_inventories.csv')
    return jsonify(data)


@app.route('/api/baltic-dry')
def get_baltic_dry():
    """Baltic Dry Index (shipping ETF proxy)"""
    data = read_csv('baltic_dry.csv')
    return jsonify(data)


@app.route('/api/status')
def status():
    """Check data status"""
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
        'consumer_sentiment', 'credit_spreads', 'dxy', 'natgas_inventories', 'baltic_dry'
    ]
    return jsonify({
        'data_dir': str(DATA_DIR),
        'last_updated': meta.get('last_updated', 'Never'),
        'files_exist': {f: (DATA_DIR / f'{f}.csv').exists() for f in files}
    })


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
        return jsonify({'success': True, 'message': 'Data refresh started. This takes 2-3 minutes. Reload the page shortly.'})
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
    app.run(debug=True, port=5003)
