import unittest

import pandas as pd

from data_fetcher import classify_vehicle_fuel, process_ev_fleet_data


class EVFleetPipelineTests(unittest.TestCase):
    def test_classifies_source_fuels(self):
        self.assertEqual(classify_vehicle_fuel('BatteryElectric'), 'EV')
        self.assertEqual(classify_vehicle_fuel('PluginHybrid'), 'EV')
        self.assertEqual(classify_vehicle_fuel('NonPluginHybrid'), 'HYB')
        self.assertEqual(classify_vehicle_fuel('MildHybrid'), 'HYB')
        self.assertEqual(classify_vehicle_fuel('Diesel'), 'ICE')
        self.assertEqual(classify_vehicle_fuel('Hydrogen'), 'ICE')
        self.assertEqual(classify_vehicle_fuel('UnknownLabel'), 'ICE')

    def test_process_ev_fleet_splits_hybrids_and_cumulates_stock(self):
        raw = pd.DataFrame(
            [
                {'YYYYMM': 202401, 'Country': 'China', 'Fuel': 'BatteryElectric', 'Value': 10},
                {'YYYYMM': 202401, 'Country': 'China', 'Fuel': 'NonPluginHybrid', 'Value': 4},
                {'YYYYMM': 202401, 'Country': 'China', 'Fuel': 'Petrol', 'Value': 6},
                {'YYYYMM': 202402, 'Country': 'China', 'Fuel': 'PluginHybrid', 'Value': 6},
                {'YYYYMM': 202402, 'Country': 'China', 'Fuel': 'Diesel', 'Value': 4},
                {'YYYYMM': 202401, 'Country': 'USA', 'Fuel': 'ZEV', 'Value': 2},
                {'YYYYMM': 202401, 'Country': 'USA', 'Fuel': 'ICE', 'Value': 8},
            ]
        )
        brent = pd.DataFrame(
            [
                {'date': '2024-01-01', 'brent': 80.0, 'brent_yoy_pct': 1.0, 'brent_mom_pct': 0.5},
                {'date': '2024-02-01', 'brent': 82.0, 'brent_yoy_pct': 2.0, 'brent_mom_pct': 2.5},
            ]
        )

        monthly, annual = process_ev_fleet_data(raw, brent)
        china = monthly[monthly['country'] == 'China'].reset_index(drop=True)

        self.assertEqual(float(china.loc[0, 'ev_sales']), 12.0)
        self.assertEqual(float(china.loc[0, 'ice_sales']), 8.0)
        self.assertEqual(float(china.loc[0, 'ev_sales_share']), 60.0)
        self.assertEqual(float(china.loc[1, 'ev_fleet']), 18.0)
        self.assertEqual(float(china.loc[1, 'total_fleet']), 30.0)
        self.assertEqual(float(china.loc[1, 'ev_fleet_share']), 60.0)
        self.assertEqual(float(china.loc[1, 'brent']), 82.0)

        china_annual = annual[(annual['country'] == 'China') & (annual['year'] == 2024)].iloc[0]
        self.assertEqual(float(china_annual['ev_sales']), 18.0)
        self.assertEqual(float(china_annual['total_sales']), 30.0)
        self.assertEqual(float(china_annual['ev_fleet_share']), 60.0)


if __name__ == '__main__':
    unittest.main()
