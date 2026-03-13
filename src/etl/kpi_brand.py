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
    
    def calculate_kpi_brand(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        kpi_brand_data = self.revenue_helper.get_kpi_brand_with_brand_metadata(
            target_year=target_year,
            target_month=target_month
        )
        
        actual_by_date = self.revenue_helper.get_actual_by_brand_channel_and_date(
            target_year=target_year,
            target_month=target_month
        )
        
        kpi_day_channel_adjustment_by_date = self.revenue_helper.get_kpi_day_channel_adjustment_by_date_and_channel(
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
        today = date.today()
        
        # Handle normal brand
        for row in kpi_brand_data:
            calendar_date = row['calendar_date']
            year = row['year']
            month = row['month']
            day = row['day']
            date_label = row['date_label']
            channel = row['channel']
            brand_name = row['brand_name']
            per_of_rev_by_brand_adj = row['per_of_rev_by_brand_adj']
            kpi_channel_initial = row['kpi_channel_initial']
            
            kpi_brand_initial = kpi_channel_initial * per_of_rev_by_brand_adj
            
            actual = Decimal(actual_by_date.get(calendar_date, {}).get(channel, {}).get(brand_name, 0.0))
            
            if calendar_date < today:
                kpi_brand_adjustment = actual
            else:
                kpi_day_channel_adjustment = kpi_day_channel_adjustment_by_date.get(calendar_date, {}).get(channel)
                if kpi_day_channel_adjustment is not None:
                    kpi_brand_adjustment = Decimal(kpi_day_channel_adjustment) * Decimal(per_of_rev_by_brand_adj)
                else:
                    kpi_brand_adjustment = None

            if calendar_date < today:
                gap = actual - Decimal(kpi_brand_initial)
            else:
                gap = Decimal('0')

            forecast = None
            if calendar_date < today:
                forecast = actual
            elif calendar_date == today:
                # forecast bottom-up
                forecast = forecast_by_brand_today.get(channel, {}).get(brand_name, Decimal('0'))
            else:
                # forecast top-down
                forecast = forecast_top_down_brand.get(calendar_date, {}).get(channel, Decimal("0")) * per_of_rev_by_brand_adj

            results.append({
                'calendar_date': calendar_date,
                'year': year,
                'month': month,
                'day': day,
                'date_label': date_label,
                'channel': channel,
                'brand_name': brand_name,
                'pct_of_rev_by_brand': Decimal(per_of_rev_by_brand_adj),
                'kpi_brand_initial': Decimal(kpi_brand_initial),
                'actual': actual,
                'gap': gap,
                'kpi_brand_adjustment': kpi_brand_adjustment,
                'forecast': forecast
            })
        
        # Handle new brand and other brand
        if new_brand_this_month:
            new_brand_records = self.get_new_brand_records(
                target_year=target_year,
                target_month=target_month,
                new_brands=new_brand_this_month,
                actual_by_date=actual_by_date,
                forecast_by_brand_today=forecast_by_brand_today,
                today=today,
                kpi_day_channel_adjustment_by_date=kpi_day_channel_adjustment_by_date
            )
            results.extend(new_brand_records)
        
        brands_in_metadata = {row['brand_name'] for row in kpi_brand_data}
        
        brands_with_actual = set()
        for calendar_date, channels in actual_by_date.items():
            for channel, brands in channels.items():
                for brand_name in brands.keys():
                    brands_with_actual.add(brand_name)
        
        brands_to_process = brands_with_actual - brands_in_metadata - new_brand_this_month
        
        if brands_to_process:
            other_brand_records = self.get_new_brand_records(
                target_year=target_year,
                target_month=target_month,
                new_brands=brands_to_process,
                actual_by_date=actual_by_date,
                forecast_by_brand_today=forecast_by_brand_today,
                today=today,
                kpi_day_channel_adjustment_by_date=kpi_day_channel_adjustment_by_date
            )
            results.extend(other_brand_records)
        
        return results
    
    def get_new_brand_records(
        self,
        target_year: int,
        target_month: int,
        new_brands: set,
        actual_by_date: Dict,
        forecast_by_brand_today: Dict,
        today: date,
        kpi_day_channel_adjustment_by_date: Dict
    ) -> List[Dict]:
        date_channel_combinations = self.revenue_helper.get_all_date_channel_combinations(
            target_year=target_year,
            target_month=target_month
        )
        
        results = []
        
        for brand_name in new_brands:
            for combo in date_channel_combinations:
                calendar_date = combo['calendar_date']
                year = combo['year']
                month = combo['month']
                day = combo['day']
                date_label = combo['date_label']
                channel = combo['channel']
                
                actual = Decimal(actual_by_date.get(calendar_date, {}).get(channel, {}).get(brand_name, 0.0))
                
                kpi_brand_initial = Decimal('0')
                
                if calendar_date > today:
                    continue
                
                if calendar_date < today:
                    kpi_brand_adjustment = actual
                else:
                    kpi_brand_adjustment = None
                
                gap = actual
                
                
                forecast = None
                if calendar_date < today:
                    forecast = actual
                elif calendar_date == today:
                    # forecast bottom-up
                    forecast = forecast_by_brand_today.get(channel, {}).get(brand_name, Decimal('0'))
                else:
                    # forecast top-down
                    forecast = Decimal('0')
                
                results.append({
                    'calendar_date': calendar_date,
                    'year': year,
                    'month': month,
                    'day': day,
                    'date_label': date_label,
                    'channel': channel,
                    'brand_name': brand_name,
                    'pct_of_rev_by_brand': Decimal('0'),
                    'kpi_brand_initial': kpi_brand_initial,
                    'actual': actual,
                    'gap': gap,
                    'kpi_brand_adjustment': kpi_brand_adjustment,
                    'forecast': forecast
                })
        
        return results
    
    def save_kpi_brand(self, kpi_brand_data: List[Dict]) -> None:
        if not kpi_brand_data:
            return
        
        now = datetime.now()
        
        data = []
        for row in kpi_brand_data:
            data.append([
                row['calendar_date'],
                row['year'],
                row['month'],
                row['day'],
                row['date_label'],
                row['channel'],
                row['brand_name'],
                row['pct_of_rev_by_brand'],
                row['kpi_brand_initial'],
                row['actual'],
                row['gap'],
                row['kpi_brand_adjustment'],
                row['forecast'],
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'day', 'date_label',
            'channel', 'brand_name', 'pct_of_rev_by_brand', 
            'kpi_brand_initial',
            'actual', 'gap', 'kpi_brand_adjustment', 'forecast', 
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_brand", data, column_names=columns)
    
    def calculate_and_save_kpi_brand(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        kpi_brand_data = self.calculate_kpi_brand(
            target_year=target_year,
            target_month=target_month
        )
        
        self.save_kpi_brand(kpi_brand_data)
        
        return kpi_brand_data


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
                target_month = int(sys.argv[i + 1])
                i += 2
            elif sys.argv[i] == "--target-year" and i + 1 < len(sys.argv):
                target_year = int(sys.argv[i + 1])
                i += 2
            else:
                i += 1
    
    if target_month is None:
        today = date.today()
        if today.year == constants.KPI_YEAR_2026:
            target_month = today.month
        else:
            target_month = 1
    
    if target_month < 1 or target_month > 12:
        print(f"Error: target_month must be between 1 and 12, received: {target_month}")
        sys.exit(1)
    
    print(f"Calculating kpi_brand for month {target_month}/{target_year}...")
    kpi_brand_data = calculator.calculate_and_save_kpi_brand(
        target_year=target_year,
        target_month=target_month
    )