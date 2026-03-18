from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIBrandCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()

    def calculate_and_save_kpi_brand(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        # 1. Fetch data at subchannel/store level
        kpi_brand_metadata_input = self.revenue_helper.get_kpi_subchannel_with_brand_metadata(
            target_year=target_year,
            target_month=target_month
        )
        
        actual_by_date = self.revenue_helper.get_actual_by_brand_subchannel_store_and_date(
            target_year=target_year,
            target_month=target_month
        )
        
        kpi_sub_adj_map = self.revenue_helper.get_kpi_subchannel_adjustment_by_date_subchannel_store(
            target_year=target_year,
            target_month=target_month
        )

        forecast_by_brand_today = self.revenue_helper.get_forecast_by_brand_for_today()

        forecast_top_down_sub = self.revenue_helper.get_forecast_top_down_from_subchannel(
            target_year=target_year,
            target_month=target_month
        )
        
        new_brand_this_month = self.revenue_helper.get_new_brand_this_month()
        
        results = []
        data_to_insert = []
        now = datetime.now()
        today = date.today()
        
        # Track processed combinations for new/other brands
        processed_combos = set() # (date, channel, subchannel, store_name, brand_name)

        # GROUP 1: Brands from metadata (Normal brands)
        for row in kpi_brand_metadata_input:
            calendar_date = row['calendar_date']
            channel = row['channel']
            subchannel = row['subchannel']
            store_name = row['store_name']
            brand_name = row['brand_name']
            pct_adj = row['per_of_rev_by_brand_adj']
            kpi_sub_init = row['kpi_subchannel_initial']
            
            processed_combos.add((calendar_date, channel, subchannel, store_name, brand_name))

            kpi_brand_init = kpi_sub_init * pct_adj
            actual = Decimal(str(actual_by_date.get(calendar_date, {}).get(channel, {}).get(subchannel, {}).get(store_name, {}).get(brand_name, 0.0)))
            
            # Adjustment
            if calendar_date < today:
                kpi_brand_adj = actual
                gap = actual - kpi_brand_init
            elif calendar_date == today:
                gap = actual - kpi_brand_init
            else:
                gap = Decimal('0')
                sub_adj = kpi_sub_adj_map.get(calendar_date, {}).get(channel, {}).get(subchannel, {}).get(store_name)
                kpi_brand_adj = Decimal(str(sub_adj)) * pct_adj if sub_adj is not None else None

            # Forecast
            if calendar_date < today:
                forecast = actual
            elif calendar_date == today:
                forecast = forecast_by_brand_today.get(channel, {}).get(brand_name, Decimal('0'))
            else:
                sub_forecast = forecast_top_down_sub.get(calendar_date, {}).get(channel, {}).get(subchannel, {}).get(store_name, Decimal('0'))
                forecast = sub_forecast * pct_adj

            res_row = {
                'calendar_date': calendar_date, 'year': row['year'], 'month': row['month'], 'day': row['day'],
                'date_label': row['date_label'], 'channel': channel, 'subchannel': subchannel, 'store_name': store_name,
                'brand_name': brand_name, 'pct_of_rev_by_brand': pct_adj, 'kpi_brand_initial': kpi_brand_init,
                'actual': actual, 'gap': gap, 'kpi_brand_adjustment': kpi_brand_adj, 'forecast': forecast
            }
            results.append(res_row)

        # GROUP 2 & 3: New Brands or Brands with actual but no metadata
        for cal_date, channels in actual_by_date.items():
            if cal_date > today: continue
            
            for channel, subchannels in channels.items():
                for subchannel, stores in subchannels.items():
                    for store_name, brands in stores.items():
                        for brand_name, actual_val in brands.items():
                            if (cal_date, channel, subchannel, store_name, brand_name) in processed_combos:
                                continue
                            
                            actual = Decimal(str(actual_val))
                            kpi_brand_init = Decimal('0')
                            kpi_brand_adj = actual if cal_date < today else None
                            
                            forecast = actual if cal_date < today else forecast_by_brand_today.get(channel, {}).get(brand_name, Decimal('0'))
                            
                            # Note: Simplified date info from actual record
                            res_row = {
                                'calendar_date': cal_date, 'year': cal_date.year, 'month': cal_date.month, 'day': cal_date.day,
                                'date_label': '', 
                                'channel': channel, 'subchannel': subchannel, 'store_name': store_name,
                                'brand_name': brand_name, 'pct_of_rev_by_brand': Decimal('0'), 'kpi_brand_initial': kpi_brand_init,
                                'actual': actual, 'gap': actual, 'kpi_brand_adjustment': kpi_brand_adj, 'forecast': forecast
                            }
                            results.append(res_row)

        # 4. Save to DB
        for row in results:
            data_to_insert.append([
                row['calendar_date'], row['year'], row['month'], row['day'], row['date_label'],
                row['channel'], row['subchannel'], row['store_name'], row['brand_name'], 
                row['pct_of_rev_by_brand'], row['kpi_brand_initial'], row['actual'], row['gap'], 
                row['kpi_brand_adjustment'], row['forecast'], now, now
            ])

        if data_to_insert:
            columns = [
                'calendar_date', 'year', 'month', 'day', 'date_label', 'channel', 'subchannel',
                'store_name', 'brand_name', 'pct_of_rev_by_brand', 'kpi_brand_initial',
                'actual', 'gap', 'kpi_brand_adjustment', 'forecast', 'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_brand", data_to_insert, column_names=columns)
            
        return results


if __name__ == "__main__":
    import sys
    constants = Constants()
    calculator = KPIBrandCalculator(constants)
    
    target_month = None
    target_year = constants.KPI_YEAR_2026
    
    if len(sys.argv) > 1:
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--target-month" and i + 1 < len(sys.argv):
                target_month = int(sys.argv[i + 1]); i += 2
            elif sys.argv[i] == "--target-year" and i + 1 < len(sys.argv):
                target_year = int(sys.argv[i + 1]); i += 2
            else: i += 1
    
    if target_month is None:
        today = date.today()
        target_month = today.month if today.year == constants.KPI_YEAR_2026 else 1
    
    print(f"Calculating kpi_brand for month {target_month}/{target_year}...")
    kpi_brand_data = calculator.calculate_and_save_kpi_brand(target_year=target_year, target_month=target_month)
    print(f"Successfully saved {len(kpi_brand_data)} kpi_brand records")