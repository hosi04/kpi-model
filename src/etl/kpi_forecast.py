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
    ) -> List:
        today = date.today()
        now = datetime.now()
        
        query = f"""
            SELECT 
                toDate(t.created_at) as calendar_date,
                CASE 
                    WHEN t.platform = 'ONLINE_HASAKI' THEN 'ONLINE_HASAKI'
                    WHEN t.platform = 'OFFLINE_HASAKI' THEN 'OFFLINE_HASAKI'
                    ELSE 'ECOM'
                END as channel,
                t.brand_name,
                CAST(t.sku AS String) AS sku_str,
                SUM(COALESCE(t.total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details AS t FINAL
            INNER JOIN (
                SELECT sku 
                FROM hskcdp.kpi_sku_metadata FINAL
                WHERE year = {target_year} AND month = {target_month}
            ) AS m ON CAST(t.sku AS String) = CAST(m.sku AS String)
            WHERE toYear(t.created_at) = {target_year}
              AND toMonth(t.created_at) = {target_month}
              AND t.status NOT IN ('Canceled', 'Cancel')
            GROUP BY calendar_date, channel, t.brand_name, sku_str
            ORDER BY calendar_date, channel, t.brand_name, sku_str
        """
        
        result = self.client.query(query)
        
        hourly_revenue_pct_by_channel = self.revenue_helper.get_hourly_revenue_percentage_by_channel(days_back=30)
        max_hour = self.revenue_helper.get_max_hour_from_transaction_details(target_year, target_month)
        cutoff_hour = max_hour if max_hour is not None else now.hour
        
        data = []
        total_actual_today = Decimal('0')
        
        for row in result.result_rows:
            calendar_date = row[0]
            channel = str(row[1])
            brand_name = str(row[2])
            sku_name = str(row[3])
            actual_amount = Decimal(str(row[4]))
            
            year, month, day = calendar_date.year, calendar_date.month, calendar_date.day
            forecast = Decimal('0')
            
            if calendar_date < today:
                forecast = actual_amount
                
            elif calendar_date == today:
                total_actual_today += actual_amount
                
                channel_pcts = hourly_revenue_pct_by_channel.get(channel, {})
                cumulative_pct = sum(Decimal(str(channel_pcts.get(h, 0.0))) for h in range(cutoff_hour + 1))

                if cumulative_pct > 0:
                    forecast = actual_amount / cumulative_pct
                else:
                    forecast = actual_amount
            
            data.append([
                calendar_date, year, month, day,
                channel, brand_name, sku_name, forecast, now
            ])
            
        print(f"DEBUG. total_actual_today_filtered: {total_actual_today}")
        
        if data:
            columns = [
                'calendar_date', 'year', 'month', 'day',
                'channel', 'brand_name', 'sku', 'forecast', 'updated_at'
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