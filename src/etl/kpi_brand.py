from decimal import Decimal
from datetime import datetime
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class KPIDayChannelBrandCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def calculate_kpi_day_channel_brand(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        # Get kpi_day_channel_initial and metadata (per_of_rev_by_brand_adj) from respective tables
        query = f"""
            SELECT 
                c.calendar_date,
                c.year,
                c.month,
                c.day,
                c.date_label,
                c.channel,
                b.brand_name,
                b.per_of_rev_by_brand_adj,
                b.per_of_rev_by_brand_adj * c.kpi_day_channel_initial AS kpi_day_channel_brand                
            FROM (SELECT * FROM hskcdp.kpi_day_channel FINAL) AS c 
            CROSS JOIN (
                SELECT 
                    brand_name,
                    per_of_rev_by_brand_adj
                FROM hskcdp.kpi_brand_metadata FINAL
                WHERE month = {target_month}
            ) AS b
            WHERE c.year = {target_year}
              AND c.month = {target_month}
              AND NOT (
                  (c.month = 6 AND c.day BETWEEN 5 AND 7) OR
                  (c.month = 9 AND c.day BETWEEN 8 AND 10) OR
                  (c.month = 11 AND c.day BETWEEN 10 AND 12) OR
                  (c.month = 12 AND c.day BETWEEN 11 AND 13)
              )
            ORDER BY c.calendar_date, c.channel, b.brand_name
        """
        
        result = self.client.query(query)
        
        results = []
        for row in result.result_rows:
            results.append({
                'calendar_date': row[0],
                'year': int(row[1]),
                'month': int(row[2]),
                'day': int(row[3]),
                'date_label': str(row[4]),
                'channel': str(row[5]),
                'brand_name': str(row[6]),
                'percentage_of_revenue_by_brand': Decimal(str(row[7])),  # Store rev_per_brand_adj
                'kpi_day_channel_brand': Decimal(str(row[8]))
            })
        
        return results
    
    def save_kpi_day_channel_brand(self, kpi_day_channel_brand_data: List[Dict]) -> None:
        if not kpi_day_channel_brand_data:
            return
        
        now = datetime.now()
        
        data = []
        for row in kpi_day_channel_brand_data:
            data.append([
                row['calendar_date'],
                row['year'],
                row['month'],
                row['day'],
                row['date_label'],
                row['channel'],
                row['brand_name'],
                row['percentage_of_revenue_by_brand'],
                row['kpi_day_channel_brand'],
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'day', 'date_label',
            'channel', 'brand_name', 'percentage_of_revenue_by_brand', 
            'kpi_day_channel_brand',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_day_channel_brand", data, column_names=columns)
    
    def calculate_and_save_kpi_day_channel_brand(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        kpi_day_channel_brand_data = self.calculate_kpi_day_channel_brand(
            target_year=target_year,
            target_month=target_month
        )
        
        self.save_kpi_day_channel_brand(kpi_day_channel_brand_data)
        
        return kpi_day_channel_brand_data


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIDayChannelBrandCalculator(constants)
    
    print("Calculating kpi_day_channel_brand for month 1/2026...")
    kpi_day_channel_brand_data = calculator.calculate_and_save_kpi_day_channel_brand(
        target_year=2026,
        target_month=1
    )