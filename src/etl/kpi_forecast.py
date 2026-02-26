from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIForecastCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()

    def calculate_forecast_bottom_up(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        today = date.today()
        current_hour = datetime.now().hour
        
        query = f"""
            SELECT 
                calendar_date,
                channel,
                brand_name,
                sku
            FROM hskcdp.kpi_sku FINAL
            WHERE toYear(calendar_date) = {target_year}
              AND toMonth(calendar_date) = {target_month}
            GROUP BY calendar_date, channel, brand_name, sku
            ORDER BY calendar_date, channel, brand_name, sku
        """
        
        result = self.client.query(query)
        
        actual_by_date = self.revenue_helper.get_actual_by_sku_brand_channel_and_date(
            target_year=target_year,
            target_month=target_month
        )
        
        hourly_revenue_pct_by_channel = self.revenue_helper.get_hourly_revenue_percentage_by_channel(days_back=30)
        
        # Lấy giờ lớn nhất có transaction trong ngày hôm nay (nếu có)
        max_hour = self.revenue_helper.get_max_hour_from_transaction_details(target_year, target_month)
        if max_hour is not None:
            cutoff_hour = max_hour
        else:
            cutoff_hour = current_hour
        
        # until_hour dùng cho get_daily_actual_until_hour: lấy từ 00:00 tới <until_hour
        until_hour = cutoff_hour + 1

        actual_by_sku_cache = {}
        
        now = datetime.now()
        data = []
        sum_check = 0
        for row in result.result_rows:
            calendar_date = row[0]
            channel = str(row[1])
            brand_name = str(row[2])
            sku_name = str(row[3])
            
            year = calendar_date.year
            month = calendar_date.month
            day = calendar_date.day
            
            # Tính forecast
            forecast = Decimal('0')
            
            if calendar_date < today:
                actual = actual_by_date.get(calendar_date, {}).get(channel, {}).get(brand_name, {}).get(sku_name, 0.0)
                if actual:
                    forecast = Decimal(str(actual))
                else:
                    forecast = Decimal('0')
                
            elif calendar_date == today:
                cache_key = calendar_date
                if cache_key not in actual_by_sku_cache:
                    # Data return: {channel: {sku: actual}}
                    actual_by_sku_cache[cache_key] = self.revenue_helper.get_daily_actual_until_hour_by_sku(
                        target_date=calendar_date,
                        until_hour=until_hour
                    )
                
                actual_until_hour = Decimal('0')
                if channel in actual_by_sku_cache[cache_key] and sku_name in actual_by_sku_cache[cache_key][channel]:
                    actual_until_hour = Decimal(str(actual_by_sku_cache[cache_key][channel][sku_name]))
                
                sum_check += actual_until_hour

                # Tính % revenue CỘNG DỒN từ 0h đến giờ cutoff cho channel này
                channel_pcts = hourly_revenue_pct_by_channel.get(channel, {})
                cumulative_pct = Decimal('0')
                for h in range(cutoff_hour + 1):
                    pct_h_raw = channel_pcts.get(h, 0.0)
                    cumulative_pct += Decimal(str(pct_h_raw))

                if cumulative_pct > Decimal('0'):
                    forecast = actual_until_hour / cumulative_pct
                else:
                    forecast = Decimal('0')
                    
            else:
                forecast = Decimal('0')
            
            
            data.append([
                calendar_date,
                year,
                month,
                day,
                channel,
                brand_name,
                sku_name,
                forecast,
                now
            ])
        print(f"DEBUG============{sum_check}")
        
        if data:
            columns = [
                'calendar_date', 'year', 'month', 'day',
                'channel', 'brand', 'sku', 'forecast', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_forecast", data, column_names=columns)
        return data

if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPIForecastCalculator(constants)
    
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
    
    print(f"Calculating forecast for month {target_month}/{target_year}...")
    forecast_data = calculator.calculate_forecast_bottom_up(
        target_year=target_year,
        target_month=target_month
    )
    
    print(f"Successfully saved {len(forecast_data)} forecast records")