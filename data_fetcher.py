"""
Data Fetcher - Pulls macro data from FRED/BEA APIs and saves to CSV
Run this to refresh data: python data_fetcher.py
"""

import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import yfinance as yf

# Load API keys - try config file first, then environment variables
try:
    from config.api_keys import API_KEYS
except ImportError:
    API_KEYS = {
        'FRED': os.environ.get('FRED_API_KEY', ''),
        'BEA': os.environ.get('BEA_API_KEY', ''),
        'EIA': os.environ.get('EIA_API_KEY', ''),
    }
    if not API_KEYS['FRED']:
        print("Warning: No API keys found - set FRED_API_KEY, BEA_API_KEY, EIA_API_KEY environment variables")

DATA_DIR = Path(__file__).parent / 'data'
FRED_URL = 'https://api.stlouisfed.org/fred/series/observations'
BEA_URL = 'https://apps.bea.gov/api/data'
EIA_URL = 'https://api.eia.gov/v2/petroleum/stoc/wstk/data'
EIA_PSM_URL = 'https://api.eia.gov/v2/petroleum/sum/snd/data'

# PSM Supply/Demand process codes
PSM_PROCESSES = {
    'production': 'FPF',      # Field Production
    'imports': 'IM0',         # Imports
    'exports': 'EEX',         # Exports
    'refinery_input': 'YIR',  # Refinery and Blender Net Input
    'refinery_output': 'YPR', # Refinery and Blender Net Production
    'product_supplied': 'VPP', # Product Supplied (demand)
    'stock_change': 'SCG',    # Stock Change
}

# Products to track for supply/demand balance
PSM_PRODUCTS = {
    'crude': 'EPC0',
    'total_gasoline': 'EPM0',
    'distillate': 'EPD0',
    'jet_fuel': 'EPJK',
    'propane': 'EPLLPZ',
}

# EIA Petroleum Product Codes (for product/duoarea facet queries)
EIA_PRODUCTS = {
    'total_gasoline': {'code': 'EPM0', 'name': 'Total Motor Gasoline', 'units': 'Thousand Barrels'},
    'distillate': {'code': 'EPD0', 'name': 'Distillate Fuel Oil', 'units': 'Thousand Barrels'},
    'jet_fuel': {'code': 'EPJK', 'name': 'Kerosene-Type Jet Fuel', 'units': 'Thousand Barrels'},
    'residual': {'code': 'EPPR', 'name': 'Residual Fuel Oil', 'units': 'Thousand Barrels'},
    'propane': {'code': 'EPLLPZ', 'name': 'Propane/Propylene', 'units': 'Thousand Barrels'},
    'ngl': {'code': 'EPL0XP', 'name': 'NGLs (excl. Propane)', 'units': 'Thousand Barrels'},
}

# EIA Series codes for crude oil EXCLUDING SPR (commercial stocks only)
EIA_CRUDE_SERIES = {
    'crude_US': {'series': 'WCESTUS1', 'name': 'Crude Oil (excl. SPR)', 'region': 'U.S. Total'},
    'crude_PADD1': {'series': 'WCESTP11', 'name': 'Crude Oil (excl. SPR)', 'region': 'PADD 1'},
    'crude_PADD2': {'series': 'WCESTP21', 'name': 'Crude Oil (excl. SPR)', 'region': 'PADD 2'},
    'crude_PADD3': {'series': 'WCESTP31', 'name': 'Crude Oil (excl. SPR)', 'region': 'PADD 3'},
    'crude_PADD4': {'series': 'WCESTP41', 'name': 'Crude Oil (excl. SPR)', 'region': 'PADD 4'},
    'crude_PADD5': {'series': 'WCESTP51', 'name': 'Crude Oil (excl. SPR)', 'region': 'PADD 5'},
}

# EIA PADD Region Codes
EIA_PADDS = {
    'US': {'code': 'NUS', 'name': 'U.S. Total'},
    'PADD1': {'code': 'R10', 'name': 'PADD 1 (East Coast)'},
    'PADD2': {'code': 'R20', 'name': 'PADD 2 (Midwest)'},
    'PADD3': {'code': 'R30', 'name': 'PADD 3 (Gulf Coast)'},
    'PADD4': {'code': 'R40', 'name': 'PADD 4 (Rocky Mountain)'},
    'PADD5': {'code': 'R50', 'name': 'PADD 5 (West Coast)'},
}


def fetch_bea_ita_series(indicator, start_year=2015):
    """Fetch a single indicator from BEA ITA dataset"""
    params = {
        'UserID': API_KEYS['BEA'],
        'method': 'GetData',
        'DatasetName': 'ITA',
        'Indicator': indicator,
        'AreaOrCountry': 'AllCountries',
        'Frequency': 'QSA',
        'Year': ','.join(str(y) for y in range(start_year, 2027)),
        'ResultFormat': 'JSON'
    }
    response = requests.get(BEA_URL, params=params)
    data = response.json()

    results = []
    if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
        if 'Data' in data['BEAAPI']['Results']:
            for row in data['BEAAPI']['Results']['Data']:
                period = row.get('TimePeriod', '')
                value = row.get('DataValue', '')
                if period and value and value != '--':
                    # Convert to billions (BEA ITA is in millions)
                    try:
                        val_billions = float(value.replace(',', '')) / 1000
                        results.append((period, val_billions))
                    except (ValueError, TypeError):
                        pass
    return results


def fetch_eia_weekly_stocks(product_code, area_code, start_date='2015-01-01'):
    """Fetch weekly petroleum stocks from EIA API v2 using product/duoarea facets"""
    all_data = []
    offset = 0
    length = 5000  # Max allowed by API

    while True:
        params = {
            'api_key': API_KEYS['EIA'],
            'frequency': 'weekly',
            'data[0]': 'value',
            'facets[product][]': product_code,
            'facets[duoarea][]': area_code,
            'start': start_date,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'offset': offset,
            'length': length
        }

        response = requests.get(EIA_URL, params=params)
        data = response.json()

        if 'response' in data and 'data' in data['response']:
            rows = data['response']['data']
            if not rows:
                break
            all_data.extend(rows)
            if len(rows) < length:
                break
            offset += length
        else:
            break

    return all_data


def fetch_eia_weekly_by_series(series_id, start_date='2015-01-01'):
    """Fetch weekly petroleum stocks from EIA API v2 using series ID"""
    all_data = []
    offset = 0
    length = 5000

    while True:
        params = {
            'api_key': API_KEYS['EIA'],
            'frequency': 'weekly',
            'data[0]': 'value',
            'facets[series][]': series_id,
            'start': start_date,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'offset': offset,
            'length': length
        }

        response = requests.get(EIA_URL, params=params)
        data = response.json()

        if 'response' in data and 'data' in data['response']:
            rows = data['response']['data']
            if not rows:
                break
            all_data.extend(rows)
            if len(rows) < length:
                break
            offset += length
        else:
            break

    return all_data


def fetch_eia_psm(product_code, process_code, area_code='NUS', start_date='2015-01-01'):
    """Fetch monthly supply/disposition data from EIA PSM"""
    all_data = []
    offset = 0
    length = 5000
    max_retries = 3

    while True:
        params = {
            'api_key': API_KEYS['EIA'],
            'frequency': 'monthly',
            'data[0]': 'value',
            'facets[product][]': product_code,
            'facets[process][]': process_code,
            'facets[duoarea][]': area_code,
            'start': start_date,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'offset': offset,
            'length': length
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(EIA_PSM_URL, params=params, timeout=30)
                data = response.json()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    print(f"    Warning: Failed after {max_retries} attempts")
                    return all_data

        if 'response' in data and 'data' in data['response']:
            rows = data['response']['data']
            if not rows:
                break
            all_data.extend(rows)
            if len(rows) < length:
                break
            offset += length
        else:
            break

        time.sleep(0.5)  # Rate limit delay

    return all_data


def fetch_fred_series(series_id, start_date='2015-01-01'):
    """Fetch a single series from FRED"""
    params = {
        'series_id': series_id,
        'api_key': API_KEYS['FRED'],
        'file_type': 'json',
        'observation_start': start_date,
        'sort_order': 'asc'
    }
    response = requests.get(FRED_URL, params=params)
    data = response.json()

    if 'observations' in data:
        obs = data['observations']
        return [(o['date'], float(o['value'])) for o in obs if o['value'] != '.']
    return []


def fetch_gdp_components():
    """Fetch GDP and components from FRED"""
    print("Fetching GDP components...")

    series = {
        'gdp': 'GDPC1',
        'consumption': 'PCECC96',
        'investment': 'GPDIC1',
        'govt_spending': 'GCEC1',
        'exports': 'EXPGSC1',
        'imports': 'IMPGSC1',
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id)
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df.to_csv(DATA_DIR / 'gdp_components.csv', index=False)
    print(f"  Saved {len(df)} rows to gdp_components.csv")
    return df


def fetch_gdp_contributions():
    """Fetch GDP growth contributions from FRED"""
    print("Fetching GDP contributions...")

    series = {
        'pce_goods': 'DGDSRY2Q224SBEA',
        'pce_services': 'DSERRY2Q224SBEA',
        'nonres_investment': 'A008RY2Q224SBEA',
        'res_investment': 'A011RY2Q224SBEA',
        'inventories': 'A014RY2Q224SBEA',
        'net_exports': 'A019RY2Q224SBEA',
        'government': 'A822RY2Q224SBEA',
        'total_gdp': 'A191RL1Q225SBEA',
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2018-04-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Add quarter column
    df['quarter'] = df['date'].dt.year.astype(str) + 'Q' + df['date'].dt.quarter.astype(str)

    df.to_csv(DATA_DIR / 'gdp_contributions.csv', index=False)
    print(f"  Saved {len(df)} rows to gdp_contributions.csv")
    return df


def fetch_trade_detail():
    """Fetch trade breakdown (goods vs services)"""
    print("Fetching trade detail...")

    series = {
        'exports_goods': 'A253RC1Q027SBEA',
        'exports_services': 'A646RC1Q027SBEA',
        'imports_goods': 'A255RC1Q027SBEA',
        'imports_services': 'B656RC1Q027SBEA',
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2018-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['quarter'] = df['date'].dt.year.astype(str) + 'Q' + df['date'].dt.quarter.astype(str)

    # Calculate totals and balances
    df['total_exports'] = df['exports_goods'] + df['exports_services']
    df['total_imports'] = df['imports_goods'] + df['imports_services']
    df['trade_balance'] = df['total_exports'] - df['total_imports']
    df['goods_balance'] = df['exports_goods'] - df['imports_goods']
    df['services_balance'] = df['exports_services'] - df['imports_services']

    df.to_csv(DATA_DIR / 'trade_detail.csv', index=False)
    print(f"  Saved {len(df)} rows to trade_detail.csv")
    return df


def fetch_trade_categories():
    """Fetch all import and export categories with matching pairs from FRED"""
    print("Fetching trade categories from FRED...")

    # All imports series (FRED/BEA NIPA)
    imports = {
        'imp_capital': 'A650RC1Q027SBEA',
        'imp_consumer': 'A652RC1Q027SBEA',
        'imp_automotive': 'B651RC1Q027SBEA',
        'imp_foods': 'B647RC1Q027SBEA',
        'imp_industrial': 'LA0000041Q027SBEA',
        'imp_petroleum': 'B648RC1Q027SBEA',
        'imp_computers': 'B852RC1Q027SBEA',
        'imp_aircraft': 'B932RC1Q027SBEA',
    }

    # All exports series (FRED/BEA NIPA)
    exports = {
        'exp_capital': 'A640RC1Q027SBEA',
        'exp_consumer': 'A642RC1Q027SBEA',
        'exp_automotive': 'B641RC1Q027SBEA',
        'exp_foods': 'B638RC1Q027SBEA',
        'exp_industrial': 'A639RC1Q027SBEA',
        'exp_petroleum': 'LA0000061Q027SBEA',
        'exp_computers': 'B850RC1Q027SBEA',
        'exp_aircraft': 'B688RC1Q027SBEA',
    }

    all_series = {**imports, **exports}
    all_data = {}

    for name, series_id in all_series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['quarter'] = df['date'].dt.year.astype(str) + 'Q' + df['date'].dt.quarter.astype(str)

    # Calculate balances for each category
    df['bal_capital'] = df['exp_capital'] - df['imp_capital']
    df['bal_consumer'] = df['exp_consumer'] - df['imp_consumer']
    df['bal_automotive'] = df['exp_automotive'] - df['imp_automotive']
    df['bal_foods'] = df['exp_foods'] - df['imp_foods']
    df['bal_industrial'] = df['exp_industrial'] - df['imp_industrial']
    df['bal_petroleum'] = df['exp_petroleum'] - df['imp_petroleum']
    df['bal_computers'] = df['exp_computers'] - df['imp_computers']
    df['bal_aircraft'] = df['exp_aircraft'] - df['imp_aircraft']

    df.to_csv(DATA_DIR / 'trade_categories.csv', index=False)
    print(f"  Saved {len(df)} rows to trade_categories.csv")
    return df


def fetch_detailed_trade():
    """Fetch detailed trade categories from BEA ITA API (pharma, metals, semiconductors, etc.)"""
    print("Fetching detailed trade from BEA ITA API...")

    # BEA ITA indicators for detailed categories
    bea_series = {
        # Pharmaceuticals
        'exp_pharma': 'ExpGdsMedDentAndPharm',
        'imp_pharma': 'ImpGdsMedDentAndPharm',
        # Precious Metals (excl gold)
        'exp_precious_metals': 'ExpGdsPrecMetalsExcNonmonGold',
        'imp_precious_metals': 'ImpGdsPrecMetalsExcNonmonGold',
        # Gold
        'exp_gold': 'ExpGdsNonmonetaryGold',
        'imp_gold': 'ImpGdsNonmonetaryGold',
        # Semiconductors
        'exp_semiconductors': 'ExpGdsSemiconductors',
        'imp_semiconductors': 'ImpGdsSemiconductors',
        # Telecom Equipment
        'exp_telecom': 'ExpGdsTelecomEquip',
        'imp_telecom': 'ImpGdsTelecomEquip',
        # Chemicals (excl pharma)
        'exp_chemicals': 'ExpGdsChemsExcMeds',
        'imp_chemicals': 'ImpGdsChemsExcMeds',
        # Iron & Steel
        'exp_steel': 'ExpGdsIronAndSteelProds',
        'imp_steel': 'ImpGdsIronAndSteelProds',
        # Apparel
        'exp_apparel': 'ExpGdsAppFootAndHouse',
        'imp_apparel': 'ImpGdsAppFootAndHouse',
    }

    all_data = {}

    for name, indicator in bea_series.items():
        data = fetch_bea_ita_series(indicator, 2015)
        for quarter, value in data:
            if quarter not in all_data:
                all_data[quarter] = {'quarter': quarter}
            all_data[quarter][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df = df.sort_values('quarter')

    # Calculate balances
    df['bal_pharma'] = df['exp_pharma'] - df['imp_pharma']
    df['bal_precious_metals'] = df['exp_precious_metals'] - df['imp_precious_metals']
    df['bal_gold'] = df['exp_gold'] - df['imp_gold']
    df['bal_semiconductors'] = df['exp_semiconductors'] - df['imp_semiconductors']
    df['bal_telecom'] = df['exp_telecom'] - df['imp_telecom']
    df['bal_chemicals'] = df['exp_chemicals'] - df['imp_chemicals']
    df['bal_steel'] = df['exp_steel'] - df['imp_steel']
    df['bal_apparel'] = df['exp_apparel'] - df['imp_apparel']

    df.to_csv(DATA_DIR / 'detailed_trade.csv', index=False)
    print(f"  Saved {len(df)} rows to detailed_trade.csv")
    return df


def fetch_services_trade():
    """Fetch services trade breakdown"""
    print("Fetching services trade...")

    # Services are in millions, we'll convert to billions
    series = {
        'exports_travel': 'IEAXSTV',
        'exports_financial': 'IEAXSF',
        'exports_ip': 'IEAXSIP',
        'exports_telecom': 'IEAXSTC',
        'imports_travel': 'IEAMSTV',
        'imports_financial': 'IEAMSF',
        'imports_telecom': 'IEAMSTC',
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            # Convert millions to billions
            all_data[date][name] = value / 1000

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['quarter'] = df['date'].dt.year.astype(str) + 'Q' + df['date'].dt.quarter.astype(str)

    # Calculate totals
    df['total_services_exports'] = df[['exports_travel', 'exports_financial', 'exports_ip', 'exports_telecom']].sum(axis=1)
    df['total_services_imports'] = df[['imports_travel', 'imports_financial', 'imports_telecom']].sum(axis=1)
    df['services_balance'] = df['total_services_exports'] - df['total_services_imports']

    df.to_csv(DATA_DIR / 'services_trade.csv', index=False)
    print(f"  Saved {len(df)} rows to services_trade.csv")
    return df


def fetch_commodities():
    """Fetch energy commodity prices"""
    print("Fetching commodities...")

    series = {
        'wti_oil': 'DCOILWTICO',
        'brent_oil': 'DCOILBRENTEU',
        'natural_gas': 'DHHNGSP',
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2020-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    df.to_csv(DATA_DIR / 'commodities.csv', index=False)
    print(f"  Saved {len(df)} rows to commodities.csv")
    return df


def fetch_petroleum_inventories():
    """Fetch weekly petroleum inventories from EIA by product and PADD"""
    print("Fetching petroleum inventories from EIA...")

    # Calculate start date (10 years ago)
    start_date = (datetime.now() - pd.Timedelta(days=365*10)).strftime('%Y-%m-%d')

    all_data = {}
    meta_rows = []

    # Fetch crude oil using series IDs (excludes SPR - commercial stocks only)
    for col_name, series_info in EIA_CRUDE_SERIES.items():
        print(f"  Fetching {series_info['name']} - {series_info['region']}...")

        rows = fetch_eia_weekly_by_series(series_info['series'], start_date)

        for row in rows:
            period = row.get('period', '')
            value = row.get('value')
            if period and value is not None:
                if period not in all_data:
                    all_data[period] = {'date': period}
                try:
                    # Convert to millions of barrels for readability
                    all_data[period][col_name] = float(value) / 1000
                except (ValueError, TypeError):
                    pass

        meta_rows.append({
            'column': col_name,
            'product': series_info['name'],
            'series_id': series_info['series'],
            'region': series_info['region'],
            'units': 'Million Barrels',
            'frequency': 'Weekly',
            'source': 'EIA Weekly Petroleum Status Report'
        })

    # Fetch other products using product/duoarea facets
    for prod_key, prod_info in EIA_PRODUCTS.items():
        for padd_key, padd_info in EIA_PADDS.items():
            col_name = f"{prod_key}_{padd_key}"
            print(f"  Fetching {prod_info['name']} - {padd_info['name']}...")

            rows = fetch_eia_weekly_stocks(prod_info['code'], padd_info['code'], start_date)

            for row in rows:
                period = row.get('period', '')
                value = row.get('value')
                if period and value is not None:
                    if period not in all_data:
                        all_data[period] = {'date': period}
                    try:
                        # Convert to millions of barrels for readability
                        all_data[period][col_name] = float(value) / 1000
                    except (ValueError, TypeError):
                        pass

            meta_rows.append({
                'column': col_name,
                'product': prod_info['name'],
                'product_code': prod_info['code'],
                'region': padd_info['name'],
                'region_code': padd_info['code'],
                'units': 'Million Barrels',
                'frequency': 'Weekly',
                'source': 'EIA Petroleum Weekly Stocks'
            })

    if not all_data:
        print("  Warning: No EIA data retrieved")
        return pd.DataFrame()

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Add time metadata columns
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['week'] = df['date'].dt.isocalendar().week

    df.to_csv(DATA_DIR / 'petroleum_inventories.csv', index=False)
    print(f"  Saved {len(df)} rows to petroleum_inventories.csv")

    # Save metadata
    meta_df = pd.DataFrame(meta_rows)
    meta_df.to_csv(DATA_DIR / 'petroleum_inventories_metadata.csv', index=False)
    print(f"  Saved metadata to petroleum_inventories_metadata.csv")

    return df


def fetch_supply_demand():
    """Fetch monthly supply/demand data from EIA PSM for key products"""
    print("Fetching supply/demand data from EIA PSM...")

    start_date = (datetime.now() - pd.Timedelta(days=365*10)).strftime('%Y-%m-%d')

    all_data = {}

    # Define which processes apply to which products
    # Crude: production, imports, exports, refinery_input, stock_change
    # Products: refinery_output, imports, exports, product_supplied, stock_change
    product_processes = {
        'crude': ['production', 'imports', 'exports', 'refinery_input', 'stock_change'],
        'total_gasoline': ['refinery_output', 'imports', 'exports', 'product_supplied', 'stock_change'],
        'distillate': ['refinery_output', 'imports', 'exports', 'product_supplied', 'stock_change'],
        'jet_fuel': ['refinery_output', 'imports', 'exports', 'product_supplied', 'stock_change'],
        'propane': ['production', 'imports', 'exports', 'product_supplied', 'stock_change'],
    }

    for prod_key, prod_code in PSM_PRODUCTS.items():
        processes = product_processes.get(prod_key, [])

        for proc_key in processes:
            proc_code = PSM_PROCESSES[proc_key]
            col_name = f"{prod_key}_{proc_key}"
            print(f"  Fetching {prod_key} - {proc_key}...")

            rows = fetch_eia_psm(prod_code, proc_code, 'NUS', start_date)
            time.sleep(0.5)  # Rate limit

            for row in rows:
                period = row.get('period', '')
                value = row.get('value')
                if period and value is not None:
                    if period not in all_data:
                        all_data[period] = {'date': period}
                    try:
                        # Convert to million barrels per day (data is in thousand barrels)
                        all_data[period][col_name] = float(value) / 1000
                    except (ValueError, TypeError):
                        pass

    # Also fetch SPR stocks (weekly, we'll take monthly averages)
    print("  Fetching SPR stocks...")
    spr_rows = fetch_eia_weekly_by_series('WCSSTUS1', start_date)
    spr_by_month = {}
    for row in spr_rows:
        period = row.get('period', '')
        value = row.get('value')
        if period and value is not None:
            month_key = period[:7]  # YYYY-MM
            if month_key not in spr_by_month:
                spr_by_month[month_key] = []
            try:
                spr_by_month[month_key].append(float(value) / 1000)  # Million barrels
            except (ValueError, TypeError):
                pass

    # Calculate SPR stock change (month over month)
    sorted_months = sorted(spr_by_month.keys())
    for i, month in enumerate(sorted_months):
        avg_spr = sum(spr_by_month[month]) / len(spr_by_month[month])
        if month in all_data:
            all_data[month]['spr_stocks'] = avg_spr
            if i > 0:
                prev_month = sorted_months[i-1]
                prev_avg = sum(spr_by_month[prev_month]) / len(spr_by_month[prev_month])
                all_data[month]['spr_change'] = avg_spr - prev_avg

    if not all_data:
        print("  Warning: No PSM data retrieved")
        return pd.DataFrame()

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Add time metadata
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'supply_demand.csv', index=False)
    print(f"  Saved {len(df)} rows to supply_demand.csv")

    return df


def fetch_weekly_balance():
    """Fetch weekly WPSR supply/demand balance from EIA for multiple products and regions"""
    print("Fetching weekly balance from EIA WPSR...")

    # Define products and their series IDs
    # Format: product_region: { component: series_id }
    # For crude: supply = production + imports, demand = refinery_input + exports
    # For products: supply = refinery_output + imports, demand = product_supplied + exports

    PRODUCTS = {
        'crude_US': {
            'production': 'WCRFPUS2',
            'imports': 'WCRIMUS2',
            'exports': 'WCREXUS2',
            'refinery_input': 'WCRRIUS2',
        },
        'crude_PADD1': {
            'imports': 'WCRIMP12',
            'exports': 'WCREXP12',
            'refinery_input': 'WCRRIP12',
        },
        'crude_PADD2': {
            'imports': 'WCRIMP22',
            'exports': 'WCREXP22',
            'refinery_input': 'WCRRIP22',
        },
        'crude_PADD3': {
            'production': 'WCRFPP32',
            'imports': 'WCRIMP32',
            'exports': 'WCREXP32',
            'refinery_input': 'WCRRIP32',
        },
        'crude_PADD4': {
            'production': 'WCRFPP42',
            'imports': 'WCRIMP42',
            'refinery_input': 'WCRRIP42',
        },
        'crude_PADD5': {
            'production': 'WCRFPP52',
            'imports': 'WCRIMP52',
            'exports': 'WCREXP52',
            'refinery_input': 'WCRRIP52',
        },
        'gasoline_US': {
            'refinery_output': 'WGFRPUS2',
            'imports': 'WGTIMUS2',
            'exports': 'WGTEXUS2',
            'product_supplied': 'WGFUPUS2',
        },
        'distillate_US': {
            'refinery_output': 'WDIRPUS2',
            'imports': 'WDIIMUS2',
            'exports': 'WDIEXUS2',
            'product_supplied': 'WDIUPUS2',
        },
        'jet_fuel_US': {
            'refinery_output': 'WKJRPUS2',
            'imports': 'WKJIMUS2',
            'exports': 'WKJEXUS2',
            'product_supplied': 'WKJUPUS2',
        },
        'propane_US': {
            'refinery_output': 'WPRRPUS2',
            'imports': 'WPRIMUS2',
            'exports': 'WPREXUS2',
            'product_supplied': 'WPRUPUS2',
        },
    }

    # SPR stocks series
    SPR_SERIES = 'WCSSTUS1'

    all_rows = []

    for product_key, series_dict in PRODUCTS.items():
        print(f"  Fetching {product_key}...")
        product_data = {}

        for component, series_id in series_dict.items():
            url = 'https://api.eia.gov/v2/petroleum/sum/sndw/data/'
            params = {
                'api_key': API_KEYS.get('EIA', ''),
                'frequency': 'weekly',
                'data[0]': 'value',
                'facets[series][]': series_id,
                'sort[0][column]': 'period',
                'sort[0][direction]': 'desc',
                'offset': 0,
                'length': 20,
            }

            try:
                response = requests.get(url, params=params, timeout=30)
                data = response.json()
                if 'response' in data and 'data' in data['response']:
                    for row in data['response']['data']:
                        period = row.get('period', '')
                        value = row.get('value')
                        if period and value is not None:
                            if period not in product_data:
                                product_data[period] = {'period': period, 'product': product_key}
                            product_data[period][component] = float(value) / 1000  # Convert to MMbpd
            except Exception as e:
                print(f"    Warning: Failed to fetch {component}: {e}")

            time.sleep(0.2)

        all_rows.extend(product_data.values())

    # Fetch SPR stocks for crude_US
    print(f"  Fetching SPR stocks...")
    url = 'https://api.eia.gov/v2/petroleum/stoc/wstk/data/'
    params = {
        'api_key': API_KEYS.get('EIA', ''),
        'frequency': 'weekly',
        'data[0]': 'value',
        'facets[series][]': SPR_SERIES,
        'sort[0][column]': 'period',
        'sort[0][direction]': 'desc',
        'offset': 0,
        'length': 20,
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        spr_data = {}
        if 'response' in data and 'data' in data['response']:
            for row in data['response']['data']:
                period = row.get('period', '')
                value = row.get('value')
                if period and value is not None:
                    spr_data[period] = float(value)

        # Add SPR change to crude_US rows
        sorted_periods = sorted(spr_data.keys())
        for i, period in enumerate(sorted_periods):
            for row in all_rows:
                if row['period'] == period and row['product'] == 'crude_US':
                    row['spr_stocks'] = spr_data[period]
                    if i > 0:
                        prev_period = sorted_periods[i-1]
                        row['spr_change'] = (spr_data[period] - spr_data[prev_period]) / 7 / 1000
    except Exception as e:
        print(f"    Warning: Failed to fetch SPR: {e}")

    if not all_rows:
        print("  Warning: No weekly balance data retrieved")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df['period'] = pd.to_datetime(df['period'])
    df = df.sort_values(['product', 'period'], ascending=[True, False])

    df.to_csv(DATA_DIR / 'weekly_balance.csv', index=False)
    print(f"  Saved {len(df)} rows to weekly_balance.csv")

    return df


def fetch_treasury_withholding():
    """Fetch Treasury tax withholding data from Fiscal Data API"""
    print("Fetching Treasury withholding data...")

    # Fetch Daily Treasury Statement deposits data (last 2 years)
    url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash"

    all_data = []
    page = 1
    page_size = 10000

    while True:
        params = {
            'filter': 'record_date:gte:2023-01-01',
            'page[size]': page_size,
            'page[number]': page,
            'sort': '-record_date'
        }

        try:
            response = requests.get(url, params=params, timeout=60)
            data = response.json()

            if 'data' in data:
                rows = data['data']
                if not rows:
                    break
                all_data.extend(rows)

                # Check if there are more pages
                meta = data.get('meta', {})
                total_pages = meta.get('total-pages', 1)
                if page >= total_pages:
                    break
                page += 1
            else:
                break
        except Exception as e:
            print(f"  Warning: Failed to fetch Treasury data: {e}")
            break

    if not all_data:
        print("  Warning: No Treasury data retrieved")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)

    # Filter for tax-related categories (using transaction_catg column)
    tax_keywords = ['Taxes -', 'Customs Duties']
    df_taxes = df[df['transaction_catg'].str.contains('|'.join(tax_keywords), case=False, na=False)]

    # Convert to numeric
    df_taxes = df_taxes.copy()
    df_taxes['transaction_today_amt'] = pd.to_numeric(df_taxes['transaction_today_amt'], errors='coerce')
    df_taxes['transaction_mtd_amt'] = pd.to_numeric(df_taxes['transaction_mtd_amt'], errors='coerce')
    df_taxes['transaction_fytd_amt'] = pd.to_numeric(df_taxes['transaction_fytd_amt'], errors='coerce')

    # Parse date
    df_taxes['record_date'] = pd.to_datetime(df_taxes['record_date'])

    # Save detailed data
    df_taxes.to_csv(DATA_DIR / 'treasury_withholding_detail.csv', index=False)
    print(f"  Saved {len(df_taxes)} rows to treasury_withholding_detail.csv")

    # Create aggregated daily totals for key categories
    # Focus on "Taxes - Withheld Individual/FICA" (main withholding category)
    df_main = df_taxes[df_taxes['transaction_catg'].str.contains('Withheld Individual/FICA', case=False, na=False)]

    if len(df_main) > 0:
        # Group by date and sum
        daily_totals = df_main.groupby('record_date').agg({
            'transaction_today_amt': 'sum',
            'transaction_mtd_amt': 'max',  # MTD is cumulative, take max
            'transaction_fytd_amt': 'max'  # FYTD is cumulative, take max
        }).reset_index()

        daily_totals.columns = ['date', 'daily_withholding', 'mtd_withholding', 'fytd_withholding']
        daily_totals = daily_totals.sort_values('date')

        # Add time columns
        daily_totals['year'] = daily_totals['date'].dt.year
        daily_totals['month'] = daily_totals['date'].dt.month
        daily_totals['day'] = daily_totals['date'].dt.day
        daily_totals['weekday'] = daily_totals['date'].dt.dayofweek

        # Calculate rolling averages
        daily_totals['daily_7d_avg'] = daily_totals['daily_withholding'].rolling(7, min_periods=1).mean()
        daily_totals['daily_30d_avg'] = daily_totals['daily_withholding'].rolling(30, min_periods=1).mean()

        daily_totals.to_csv(DATA_DIR / 'treasury_withholding.csv', index=False)
        print(f"  Saved {len(daily_totals)} rows to treasury_withholding.csv")
    else:
        print("  Warning: No 'Withheld Individual/FICA' data found")

    return df_taxes


def fetch_transportation():
    """Fetch transportation data - air passengers and VMT from FRED"""
    print("Fetching transportation data...")

    series = {
        'air_passengers': 'LOADFACTOR',      # Air Load Factor (proxy for demand)
        'rail_passengers': 'RAILPM',         # Rail Passenger Miles
        'vmt': 'TRFVOLUSM227NFWA',            # Vehicle Miles Traveled
    }

    all_data = {}
    for name, series_id in series.items():
        try:
            data = fetch_fred_series(series_id, '2015-01-01')
            for date, value in data:
                if date not in all_data:
                    all_data[date] = {'date': date}
                all_data[date][name] = value
        except Exception as e:
            print(f"  Warning: Could not fetch {name} ({series_id}): {e}")

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Add time columns
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'transportation.csv', index=False)
    print(f"  Saved {len(df)} rows to transportation.csv")
    return df


def fetch_refinery_runs():
    """Fetch US refinery net input from EIA PSM (Petroleum Supply Monthly)"""
    print("Fetching refinery runs data...")

    # EIA API series for monthly refinery data
    series = {
        'crude_input': 'MCRRIUS2',    # US Crude Oil Net Input to Refineries (Mbpd)
    }

    url = 'https://api.eia.gov/v2/petroleum/sum/snd/data/'
    all_data = {}

    for name, series_id in series.items():
        params = {
            'api_key': API_KEYS.get('EIA', ''),
            'frequency': 'monthly',
            'data[0]': 'value',
            'facets[series][]': series_id,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'desc',
            'length': 5000
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                data = resp.json().get('response', {}).get('data', [])
                for row in data:
                    period = row.get('period')
                    value = row.get('value')
                    if period and value is not None:
                        if period not in all_data:
                            all_data[period] = {}
                        try:
                            all_data[period][name] = float(value)
                        except (ValueError, TypeError):
                            pass
                print(f"  {name}: {len(data)} records")
        except Exception as e:
            print(f"  Error fetching {name}: {e}")

    if not all_data:
        print("  No refinery data fetched")
        return None

    # Convert to DataFrame
    rows = []
    for period, values in all_data.items():
        row = {'date': f"{period}-01", **values}
        rows.append(row)

    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'refinery_runs.csv', index=False)
    print(f"  Saved {len(df)} rows to refinery_runs.csv")
    return df


def fetch_employment():
    """Fetch employment data by state and metro area from FRED"""
    print("Fetching employment data...")

    # National and major state employment (Nonfarm Payrolls, thousands)
    series = {
        'us_total': 'PAYEMS',           # Total US Nonfarm
        'us_private': 'USPRIV',         # US Private
        'california': 'CANA',           # California
        'texas': 'TXNA',                # Texas
        'new_york': 'NYNA',             # New York
        'florida': 'FLNA',              # Florida
        'illinois': 'ILNA',             # Illinois
        'pennsylvania': 'PANA',         # Pennsylvania
        'ohio': 'OHNA',                 # Ohio
        'georgia': 'GANA',              # Georgia
        'michigan': 'MINA',             # Michigan
        'washington': 'WANA',           # Washington
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Add time columns
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'employment.csv', index=False)
    print(f"  Saved {len(df)} rows to employment.csv")
    return df


def fetch_jolts():
    """Fetch JOLTS data from FRED"""
    print("Fetching JOLTS data...")

    series = {
        'job_openings': 'JTSJOL',       # Job Openings Total
        'hires': 'JTSHIL',              # Hires Total
        'quits': 'JTSQUL',              # Quits Total
        'layoffs': 'JTSLDL',            # Layoffs & Discharges
        'separations': 'JTSTSL',        # Total Separations
        'openings_rate': 'JTSJOR',      # Job Openings Rate
        'hires_rate': 'JTSHIR',         # Hires Rate
        'quits_rate': 'JTSQUR',         # Quits Rate
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'jolts.csv', index=False)
    print(f"  Saved {len(df)} rows to jolts.csv")
    return df


def fetch_cpi():
    """Fetch CPI data from FRED"""
    print("Fetching CPI data...")

    series = {
        'cpi_all': 'CPIAUCSL',          # CPI All Items
        'cpi_core': 'CPILFESL',         # CPI Core (less food & energy)
        'cpi_food': 'CPIUFDSL',         # CPI Food
        'cpi_energy': 'CPIENGSL',       # CPI Energy
        'cpi_shelter': 'CUSR0000SAH1',  # CPI Shelter
        'cpi_medical': 'CPIMEDSL',      # CPI Medical Care
        'cpi_transportation': 'CPITRNSL',  # CPI Transportation
        'cpi_apparel': 'CPIAPPSL',      # CPI Apparel
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate YoY % change for each series
    for col in series.keys():
        if col in df.columns:
            df[f'{col}_yoy'] = df[col].pct_change(12) * 100

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'cpi.csv', index=False)
    print(f"  Saved {len(df)} rows to cpi.csv")
    return df


def fetch_ppi():
    """Fetch PPI data from FRED"""
    print("Fetching PPI data...")

    series = {
        'ppi_all': 'PPIACO',            # PPI All Commodities
        'ppi_final_demand': 'PPIFIS',   # PPI Final Demand
        'ppi_core': 'PPIFES',           # PPI Final Demand less foods & energy
        'ppi_foods': 'PPIFCF',          # PPI Final Demand Foods
        'ppi_energy': 'PPIFCG',         # PPI Final Demand Energy
        'ppi_goods': 'PPIFGS',          # PPI Final Demand Goods
        'ppi_services': 'PPIFSS',       # PPI Final Demand Services
        'ppi_construction': 'PPIFCN',   # PPI Final Demand Construction
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate YoY % change for each series
    for col in series.keys():
        if col in df.columns:
            df[f'{col}_yoy'] = df[col].pct_change(12) * 100

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'ppi.csv', index=False)
    print(f"  Saved {len(df)} rows to ppi.csv")
    return df


def fetch_market_prices():
    """Fetch daily market prices from Yahoo Finance - 3 years of data"""
    print("Fetching market prices from Yahoo Finance...")

    # Yahoo Finance symbols
    symbols = {
        'crude_oil': 'CL=F',      # WTI Crude Oil Futures
        'nat_gas': 'NG=F',        # Natural Gas Futures (Henry Hub)
        'sp500': '^GSPC',         # S&P 500 Index
        'gold': 'GC=F',           # Gold Futures
        'silver': 'SI=F',         # Silver Futures
        'copper': 'HG=F',         # Copper Futures
    }

    end_date = datetime.now()
    start_date = end_date - timedelta(days=3*365)

    all_data = {}
    for name, symbol in symbols.items():
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date, interval='1d')
            if len(hist) > 0:
                for date, row in hist.iterrows():
                    date_str = date.strftime('%Y-%m-%d')
                    if date_str not in all_data:
                        all_data[date_str] = {'date': date_str}
                    all_data[date_str][name] = row['Close']
                print(f"  {name}: {len(hist)} records")
            else:
                print(f"  {name}: No data")
        except Exception as e:
            print(f"  Error fetching {name}: {e}")
        time.sleep(0.5)  # Rate limiting

    if not all_data:
        print("  No market data fetched")
        return None

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate 30-day moving averages
    for name in symbols.keys():
        if name in df.columns:
            df[f'{name}_ma30'] = df[name].rolling(window=30, min_periods=1).mean()

    df.to_csv(DATA_DIR / 'market_prices.csv', index=False)
    print(f"  Saved {len(df)} rows to market_prices.csv")
    return df


def fetch_oil_stocks():
    """Fetch WTI crude and top 20 US oil & gas company stock prices, normalized"""
    print("Fetching oil & gas stock prices...")

    # WTI Crude + Top 20 US Oil & Gas companies by market cap
    symbols = {
        'WTI': 'CL=F',           # WTI Crude Oil Futures
        'XOM': 'XOM',            # Exxon Mobil
        'CVX': 'CVX',            # Chevron
        'COP': 'COP',            # ConocoPhillips
        'EOG': 'EOG',            # EOG Resources
        'SLB': 'SLB',            # Schlumberger
        'MPC': 'MPC',            # Marathon Petroleum
        'PSX': 'PSX',            # Phillips 66
        'VLO': 'VLO',            # Valero Energy
        'OXY': 'OXY',            # Occidental Petroleum
        'KMI': 'KMI',            # Kinder Morgan
        'WMB': 'WMB',            # Williams Companies
        'HES': 'HES',            # Hess Corporation
        'DVN': 'DVN',            # Devon Energy
        'BKR': 'BKR',            # Baker Hughes
        'HAL': 'HAL',            # Halliburton
        'FANG': 'FANG',          # Diamondback Energy
        'CTRA': 'CTRA',          # Coterra Energy
        'MRO': 'MRO',            # Marathon Oil
        'APA': 'APA',            # APA Corporation
        'OVV': 'OVV',            # Ovintiv
    }

    end_date = datetime.now()
    start_date = end_date - timedelta(days=8*365)

    all_data = {}
    for name, symbol in symbols.items():
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date, interval='1d')
            if len(hist) > 0:
                for date, row in hist.iterrows():
                    date_str = date.strftime('%Y-%m-%d')
                    if date_str not in all_data:
                        all_data[date_str] = {'date': date_str}
                    all_data[date_str][name] = row['Close']
                print(f"  {name}: {len(hist)} records")
            else:
                print(f"  {name}: No data")
        except Exception as e:
            print(f"  Error fetching {name}: {e}")
        time.sleep(0.3)

    if not all_data:
        print("  No oil stock data fetched")
        return None

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Normalize all prices to 100 at the start of the period
    for col in symbols.keys():
        if col in df.columns:
            first_valid = df[col].first_valid_index()
            if first_valid is not None:
                base_value = df.loc[first_valid, col]
                if base_value and base_value != 0:
                    df[f'{col}_norm'] = (df[col] / base_value) * 100

    df.to_csv(DATA_DIR / 'oil_stocks.csv', index=False)
    print(f"  Saved {len(df)} rows to oil_stocks.csv")
    return df


def fetch_treasury_yields():
    """Fetch Treasury yields and yield curve from FRED"""
    print("Fetching Treasury yields...")

    series = {
        'yield_2yr': 'DGS2',           # 2-Year Treasury
        'yield_10yr': 'DGS10',         # 10-Year Treasury
        'yield_30yr': 'DGS30',         # 30-Year Treasury
        'yield_3mo': 'DGS3MO',         # 3-Month Treasury
        'yield_1yr': 'DGS1',           # 1-Year Treasury
        'yield_5yr': 'DGS5',           # 5-Year Treasury
        'fed_funds': 'FEDFUNDS',       # Fed Funds Rate
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate yield curve spreads
    if 'yield_10yr' in df.columns and 'yield_2yr' in df.columns:
        df['spread_2s10s'] = df['yield_10yr'] - df['yield_2yr']
    if 'yield_10yr' in df.columns and 'yield_3mo' in df.columns:
        df['spread_3m10y'] = df['yield_10yr'] - df['yield_3mo']

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'treasury_yields.csv', index=False)
    print(f"  Saved {len(df)} rows to treasury_yields.csv")
    return df


def fetch_jobless_claims():
    """Fetch initial and continuing jobless claims from FRED"""
    print("Fetching jobless claims...")

    series = {
        'initial_claims': 'ICSA',           # Initial Claims (weekly, SA)
        'continued_claims': 'CCSA',         # Continued Claims (weekly, SA)
        'insured_unemployment': 'IURSA',    # Insured Unemployment Rate
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate 4-week moving average for initial claims
    if 'initial_claims' in df.columns:
        df['initial_claims_4wma'] = df['initial_claims'].rolling(window=4, min_periods=1).mean()

    df['year'] = df['date'].dt.year
    df['week'] = df['date'].dt.isocalendar().week

    df.to_csv(DATA_DIR / 'jobless_claims.csv', index=False)
    print(f"  Saved {len(df)} rows to jobless_claims.csv")
    return df


def fetch_ism_pmi():
    """Fetch ISM Manufacturing and Services PMI from FRED"""
    print("Fetching ISM PMI data...")

    series = {
        'ism_manufacturing': 'MANEMP',      # ISM Manufacturing: Employment
        'ism_mfg_pmi': 'NAPM',              # ISM Manufacturing PMI
        'ism_mfg_new_orders': 'NAPMNOI',    # ISM Manufacturing New Orders
        'ism_mfg_production': 'NAPMPI',     # ISM Manufacturing Production
        'ism_mfg_prices': 'NAPMPRI',        # ISM Manufacturing Prices
        'ism_services': 'NMFCI',            # ISM Non-Manufacturing (Services) Index
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'ism_pmi.csv', index=False)
    print(f"  Saved {len(df)} rows to ism_pmi.csv")
    return df


def fetch_housing():
    """Fetch housing market data from FRED"""
    print("Fetching housing data...")

    series = {
        'housing_starts': 'HOUST',              # Housing Starts (thousands, SAAR)
        'building_permits': 'PERMIT',           # Building Permits (thousands, SAAR)
        'existing_home_sales': 'EXHOSLUSM495S', # Existing Home Sales (millions, SAAR)
        'new_home_sales': 'HSN1F',              # New Home Sales (thousands, SAAR)
        'median_home_price': 'MSPUS',           # Median Sales Price of Houses Sold
        'case_shiller': 'CSUSHPINSA',           # Case-Shiller Home Price Index
        'mortgage_rate_30yr': 'MORTGAGE30US',   # 30-Year Fixed Mortgage Rate
        'mortgage_rate_15yr': 'MORTGAGE15US',   # 15-Year Fixed Mortgage Rate
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate YoY change for Case-Shiller
    if 'case_shiller' in df.columns:
        df['case_shiller_yoy'] = df['case_shiller'].pct_change(12) * 100

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'housing.csv', index=False)
    print(f"  Saved {len(df)} rows to housing.csv")
    return df


def fetch_retail_sales():
    """Fetch retail sales data from FRED"""
    print("Fetching retail sales...")

    series = {
        'retail_total': 'RSAFS',            # Total Retail Sales (millions)
        'retail_ex_auto': 'RSFSXMV',        # Retail Sales ex. Motor Vehicles
        'retail_ex_auto_gas': 'MARTSSM44W72USS', # Retail ex. Auto & Gas Stations
        'retail_food_services': 'RSFSDP',   # Food Services & Drinking Places
        'retail_ecommerce': 'ECOMSA',       # E-Commerce Retail Sales
        'retail_building_materials': 'RSBMGESD', # Building Materials & Garden
        'retail_general_merch': 'RSGMSN',   # General Merchandise
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate MoM and YoY % changes
    for col in ['retail_total', 'retail_ex_auto']:
        if col in df.columns:
            df[f'{col}_mom'] = df[col].pct_change(1) * 100
            df[f'{col}_yoy'] = df[col].pct_change(12) * 100

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'retail_sales.csv', index=False)
    print(f"  Saved {len(df)} rows to retail_sales.csv")
    return df


def fetch_consumer_sentiment():
    """Fetch consumer sentiment data from FRED"""
    print("Fetching consumer sentiment...")

    series = {
        'umich_sentiment': 'UMCSENT',           # U of Michigan Consumer Sentiment
        'umich_expectations': 'MICH',           # U of Michigan Inflation Expectations
        'umich_current': 'UMCSENT1',            # U of Michigan Current Conditions
        'conf_board': 'CSCICP03USM665S',        # Conference Board Consumer Confidence
    }

    all_data = {}
    for name, series_id in series.items():
        data = fetch_fred_series(series_id, '2015-01-01')
        for date, value in data:
            if date not in all_data:
                all_data[date] = {'date': date}
            all_data[date][name] = value

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    df.to_csv(DATA_DIR / 'consumer_sentiment.csv', index=False)
    print(f"  Saved {len(df)} rows to consumer_sentiment.csv")
    return df


def fetch_credit_spreads():
    """Fetch credit spread proxies from Yahoo Finance (HYG, LQD ETFs)"""
    print("Fetching credit spreads...")

    # Use bond ETFs as proxies for credit conditions
    symbols = {
        'hyg': 'HYG',           # iShares High Yield Corporate Bond ETF
        'lqd': 'LQD',           # iShares Investment Grade Corporate Bond ETF
        'tlt': 'TLT',           # iShares 20+ Year Treasury Bond ETF
        'shy': 'SHY',           # iShares 1-3 Year Treasury Bond ETF
        'jnk': 'JNK',           # SPDR High Yield Bond ETF
    }

    end_date = datetime.now()
    start_date = end_date - timedelta(days=5*365)

    all_data = {}
    for name, symbol in symbols.items():
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date, interval='1d')
            if len(hist) > 0:
                for date, row in hist.iterrows():
                    date_str = date.strftime('%Y-%m-%d')
                    if date_str not in all_data:
                        all_data[date_str] = {'date': date_str}
                    all_data[date_str][name] = row['Close']
                print(f"  {name}: {len(hist)} records")
            else:
                print(f"  {name}: No data")
        except Exception as e:
            print(f"  Error fetching {name}: {e}")
        time.sleep(0.5)

    if not all_data:
        print("  No credit spread data fetched")
        return None

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate spread proxy (HYG vs LQD ratio - lower = tighter spreads)
    if 'hyg' in df.columns and 'lqd' in df.columns:
        df['hyg_lqd_ratio'] = df['hyg'] / df['lqd']

    df.to_csv(DATA_DIR / 'credit_spreads.csv', index=False)
    print(f"  Saved {len(df)} rows to credit_spreads.csv")
    return df


def fetch_dxy():
    """Fetch US Dollar Index from Yahoo Finance"""
    print("Fetching Dollar Index (DXY)...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=5*365)

    all_data = {}
    try:
        ticker = yf.Ticker('DX-Y.NYB')  # US Dollar Index
        hist = ticker.history(start=start_date, end=end_date, interval='1d')
        if len(hist) > 0:
            for date, row in hist.iterrows():
                date_str = date.strftime('%Y-%m-%d')
                all_data[date_str] = {
                    'date': date_str,
                    'dxy': row['Close'],
                    'dxy_high': row['High'],
                    'dxy_low': row['Low'],
                }
            print(f"  DXY: {len(hist)} records")
        else:
            print("  DXY: No data")
    except Exception as e:
        print(f"  Error fetching DXY: {e}")

    if not all_data:
        print("  No DXY data fetched")
        return None

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate moving averages
    df['dxy_ma50'] = df['dxy'].rolling(window=50, min_periods=1).mean()
    df['dxy_ma200'] = df['dxy'].rolling(window=200, min_periods=1).mean()

    df.to_csv(DATA_DIR / 'dxy.csv', index=False)
    print(f"  Saved {len(df)} rows to dxy.csv")
    return df


def fetch_natgas_inventories():
    """Fetch natural gas storage data from EIA"""
    print("Fetching natural gas inventories...")

    # EIA Natural Gas Weekly Storage - use series facet for US total
    url = 'https://api.eia.gov/v2/natural-gas/stor/wkly/data/'

    all_data = []
    offset = 0
    length = 5000

    while True:
        params = {
            'api_key': API_KEYS.get('EIA', ''),
            'frequency': 'weekly',
            'data[0]': 'value',
            'facets[series][]': 'NW2_EPG0_SWO_R48_BCF',  # Lower 48 Working Gas Total
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'offset': offset,
            'length': length
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()

            if 'response' in data and 'data' in data['response']:
                rows = data['response']['data']
                if not rows:
                    break
                all_data.extend(rows)
                if len(rows) < length:
                    break
                offset += length
            else:
                break
        except Exception as e:
            print(f"  Error fetching nat gas data: {e}")
            break

    if not all_data:
        print("  Warning: No natural gas data retrieved")
        return pd.DataFrame()

    # Process data
    processed = {}
    for row in all_data:
        period = row.get('period', '')
        value = row.get('value')
        if period and value is not None:
            try:
                processed[period] = {'date': period, 'storage_bcf': float(value)}
            except (ValueError, TypeError):
                pass

    df = pd.DataFrame(list(processed.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Add time columns for seasonal analysis
    df['year'] = df['date'].dt.year
    df['week'] = df['date'].dt.isocalendar().week
    df['month'] = df['date'].dt.month

    # Calculate week-over-week change
    df['storage_change'] = df['storage_bcf'].diff()

    df.to_csv(DATA_DIR / 'natgas_inventories.csv', index=False)
    print(f"  Saved {len(df)} rows to natgas_inventories.csv")
    return df


def fetch_baltic_dry():
    """Fetch Baltic Dry Index from Yahoo Finance"""
    print("Fetching Baltic Dry Index...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=5*365)

    # There's no direct BDI ticker on Yahoo, but we can use shipping ETFs
    # BDRY tracks the Baltic Dry Index
    symbols = {
        'bdry': 'BDRY',         # Breakwave Dry Bulk Shipping ETF (tracks BDI)
        'sblk': 'SBLK',         # Star Bulk Carriers
        'gogl': 'GOGL',         # Golden Ocean Group
    }

    all_data = {}
    for name, symbol in symbols.items():
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date, interval='1d')
            if len(hist) > 0:
                for date, row in hist.iterrows():
                    date_str = date.strftime('%Y-%m-%d')
                    if date_str not in all_data:
                        all_data[date_str] = {'date': date_str}
                    all_data[date_str][name] = row['Close']
                print(f"  {name}: {len(hist)} records")
            else:
                print(f"  {name}: No data")
        except Exception as e:
            print(f"  Error fetching {name}: {e}")
        time.sleep(0.5)

    if not all_data:
        print("  No Baltic Dry data fetched")
        return None

    df = pd.DataFrame(list(all_data.values()))
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Calculate moving averages for BDRY (BDI proxy)
    if 'bdry' in df.columns:
        df['bdry_ma30'] = df['bdry'].rolling(window=30, min_periods=1).mean()

    df.to_csv(DATA_DIR / 'baltic_dry.csv', index=False)
    print(f"  Saved {len(df)} rows to baltic_dry.csv")
    return df


def fetch_all():
    """Fetch all data and save to CSV"""
    print("=" * 50)
    print("Macro Data Fetcher")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 50)

    DATA_DIR.mkdir(exist_ok=True)

    fetch_gdp_components()
    fetch_gdp_contributions()
    fetch_trade_detail()
    fetch_trade_categories()
    fetch_detailed_trade()
    fetch_services_trade()
    fetch_commodities()
    fetch_petroleum_inventories()
    fetch_supply_demand()
    fetch_weekly_balance()
    fetch_treasury_withholding()
    fetch_transportation()
    fetch_refinery_runs()
    fetch_employment()
    fetch_jolts()
    fetch_cpi()
    fetch_ppi()
    fetch_market_prices()
    fetch_oil_stocks()
    fetch_treasury_yields()
    fetch_jobless_claims()
    fetch_ism_pmi()
    fetch_housing()
    fetch_retail_sales()
    fetch_consumer_sentiment()
    fetch_credit_spreads()
    fetch_dxy()
    fetch_natgas_inventories()
    fetch_baltic_dry()

    # Save metadata
    meta = {
        'last_updated': datetime.now().isoformat(),
        'files': [
            'gdp_components.csv',
            'gdp_contributions.csv',
            'trade_detail.csv',
            'trade_categories.csv',
            'detailed_trade.csv',
            'services_trade.csv',
            'commodities.csv',
            'petroleum_inventories.csv',
            'petroleum_inventories_metadata.csv',
            'supply_demand.csv',
            'weekly_balance.csv',
            'treasury_withholding.csv',
            'treasury_withholding_detail.csv',
            'transportation.csv',
            'refinery_runs.csv',
            'employment.csv',
            'jolts.csv',
            'cpi.csv',
            'ppi.csv',
            'market_prices.csv',
            'oil_stocks.csv',
            'treasury_yields.csv',
            'jobless_claims.csv',
            'ism_pmi.csv',
            'housing.csv',
            'retail_sales.csv',
            'consumer_sentiment.csv',
            'credit_spreads.csv',
            'dxy.csv',
            'natgas_inventories.csv',
            'baltic_dry.csv'
        ]
    }
    pd.DataFrame([meta]).to_csv(DATA_DIR / 'metadata.csv', index=False)

    print("=" * 50)
    print("Done!")
    print("=" * 50)


if __name__ == '__main__':
    fetch_all()
