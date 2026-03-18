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
        kpi_brand_metadata = self.revenue_helper.get_kpi_brand_with_brand_metadata(
            target_year=target_year,
            target_month=target_month
        )
        
        actual_by_date = self.revenue_helper.get_actual_by_brand_channel_and_date(
            target_year=target_year,
            target_month=target_month
        )
        
        kpi_day_channel_adj_by_date = self.revenue_helper.get_kpi_day_channel_adjustment_by_date_and_channel(
            target_year=target_year,
            target_month=target_month
        )

        forecast_by_brand_today = self.revenue_helper.get_forecast_by_brand_for_today()

        forecast_top_down_brand = self.revenue_helper.get_forecast_top_down_from_channel(
            target_year=target_year,
            target_month=target_month
        )
        
        new_brand_this_month = self.revenue_helper.get_new_brand_this_month()
        

        results = []
        data_to_insert = []
        now = datetime.now()
        today = date.today()
        brands_in_metadata = set()

        # GROUP 1: Normal brands (from metadata)
        for row in kpi_brand_metadata:
            calendar_date = row['calendar_date']
            channel = row['channel']
            brand_name = row['brand_name']
            brands_in_metadata.add(brand_name)
            pct_adj = row['per_of_rev_by_brand_adj']
            kpi_channel_init = row['kpi_channel_initial']
            
            kpi_brand_init = kpi_channel_init * pct_adj
            actual = Decimal(str(actual_by_date.get(calendar_date, {}).get(channel, {}).get(brand_name, 0.0)))
            
            # KPI Adjustment
            if calendar_date < today:
                kpi_brand_adj = actual
                gap = actual - kpi_brand_init
            else:
                gap = Decimal('0')
                channel_adj = kpi_day_channel_adj_by_date.get(calendar_date, {}).get(channel)
                kpi_brand_adj = Decimal(str(channel_adj)) * pct_adj if channel_adj is not None else None

            # Forecast
            if calendar_date < today:
                forecast = actual
            elif calendar_date == today:
                forecast = forecast_by_brand_today.get(channel, {}).get(brand_name, Decimal('0'))
            else:
                forecast = forecast_top_down_brand.get(calendar_date, {}).get(channel, Decimal("0")) * pct_adj

            res_row = {
                'calendar_date': calendar_date, 'year': row['year'], 'month': row['month'], 'day': row['day'],
                'date_label': row['date_label'], 'channel': channel, 'brand_name': brand_name,
                'pct_of_rev_by_brand': pct_adj, 'kpi_brand_initial': kpi_brand_init,
                'actual': actual, 'gap': gap, 'kpi_brand_adjustment': kpi_brand_adj, 'forecast': forecast
            }
            results.append(res_row)

        # GROUP 2 & 3: New brands and Other brands (No metadata)
        brands_with_actual = set()
        for channels in actual_by_date.values():
            for brands in channels.values():
                brands_with_actual.update(brands.keys())
        
        brands_no_metadata = (new_brand_this_month | brands_with_actual) - brands_in_metadata
        
        if brands_no_metadata:
            date_channel_combos = self.revenue_helper.get_all_date_channel_combinations(target_year, target_month)
            for brand_name in brands_no_metadata:
                for combo in date_channel_combos:
                    cal_date = combo['calendar_date']
                    if cal_date > today: continue # Skip forecast/adjustment for non-metadata brands in future
                    
                    channel = combo['channel']
                    actual = Decimal(str(actual_by_date.get(cal_date, {}).get(channel, {}).get(brand_name, 0.0)))
                    
                    # New/Other brands don't have initial KPI
                    kpi_brand_init = Decimal('0')
                    kpi_brand_adj = actual if cal_date < today else None
                    forecast = actual if cal_date < today else forecast_by_brand_today.get(channel, {}).get(brand_name, Decimal('0'))

                    res_row = {
                        'calendar_date': cal_date, 'year': combo['year'], 'month': combo['month'], 'day': combo['day'],
                        'date_label': combo['date_label'], 'channel': channel, 'brand_name': brand_name,
                        'pct_of_rev_by_brand': Decimal('0'), 'kpi_brand_initial': kpi_brand_init,
                        'actual': actual, 'gap': actual, 'kpi_brand_adjustment': kpi_brand_adj, 'forecast': forecast
                    }
                    results.append(res_row)

        # 3. Prepare data_to_insert and Save to DB
        for row in results:
            data_to_insert.append([
                row['calendar_date'], row['year'], row['month'], row['day'], row['date_label'],
                row['channel'], row['brand_name'], row['pct_of_rev_by_brand'], row['kpi_brand_initial'],
                row['actual'], row['gap'], row['kpi_brand_adjustment'], row['forecast'], now, now
            ])

        if data_to_insert:
            columns = [
                'calendar_date', 'year', 'month', 'day', 'date_label', 'channel', 'brand_name',
                'pct_of_rev_by_brand', 'kpi_brand_initial', 'actual', 'gap', 'kpi_brand_adjustment',
                'forecast', 'created_at', 'updated_at'
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
    kpi_brand_data = calculator.calculate_and_save_kpi_brand(target_year, target_month)
    print(f"Successfully saved {len(kpi_brand_data)} kpi_brand records")