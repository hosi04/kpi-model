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

        new_brand_this_month = self.revenue_helper.get_new_brand_this_month()
        
        results = []
        today = date.today()
        
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
            
            if brand_name in new_brand_this_month:
                kpi_brand_initial = Decimal('0')
                kpi_brand_adjustment = actual
            else:
                if calendar_date < today:
                    kpi_brand_adjustment = actual
                    # gap = actual - Decimal(kpi_brand_initial)
                else:
                    # gap = Decimal('0')
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
                forecast = forecast_by_brand_today.get(channel, {}).get(brand_name, Decimal('0'))
            else:
                forecast = Decimal('0')

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
    constants = Constants()
    calculator = KPIBrandCalculator(constants)
    
    print("Calculating kpi_brand for month...")
    kpi_brand_data = calculator.calculate_and_save_kpi_brand(
        target_year=2026,
        target_month=2
    )