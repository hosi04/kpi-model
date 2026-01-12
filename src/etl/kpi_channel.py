from decimal import Decimal
from datetime import datetime
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class KPIDayChannelCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def calculate_kpi_day_channel(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        # Get kpi_day_initial and metadata (revenue_percentage_adj) from respective tables
        query = f"""
            SELECT 
                kd.calendar_date,
                kd.year,
                kd.month,
                kd.day,
                kd.date_label,
                md.channel,
                md.revenue_percentage_adj,
                kd.kpi_day_initial
            FROM (SELECT * FROM hskcdp.kpi_day FINAL) AS kd
            INNER JOIN (SELECT * FROM hskcdp.kpi_day_channel_metadata FINAL) AS md
                ON kd.calendar_date = md.calendar_date
                AND kd.year = md.year
                AND kd.month = md.month
                AND kd.day = md.day
                AND kd.date_label = md.date_label
            WHERE kd.year = {target_year}
              AND kd.month = {target_month}
              AND NOT (
                  (kd.month = 6 AND kd.day BETWEEN 5 AND 7) OR
                  (kd.month = 9 AND kd.day BETWEEN 8 AND 10) OR
                  (kd.month = 11 AND kd.day BETWEEN 10 AND 12) OR
                  (kd.month = 12 AND kd.day BETWEEN 11 AND 13)
              )
            ORDER BY kd.calendar_date, md.channel
        """
        
        result = self.client.query(query)
        
        results = []
        
        for row in result.result_rows:
            calendar_date = row[0]
            year = int(row[1])
            month = int(row[2])
            day = int(row[3])
            date_label = str(row[4])
            channel = str(row[5])
            revenue_percentage_adj = Decimal(str(row[6]))
            kpi_day_initial = Decimal(str(row[7]))
            
            # Calculate kpi_day_channel using revenue_percentage_adj
            kpi_day_channel = kpi_day_initial * revenue_percentage_adj
            
            results.append({
                'calendar_date': calendar_date,
                'year': year,
                'month': month,
                'day': day,
                'date_label': date_label,
                'channel': channel,
                'revenue_percentage': float(revenue_percentage_adj),  # Store the adjusted percentage
                'kpi_day_channel_initial': float(kpi_day_channel)
            })
        
        return results
    
    def save_kpi_day_channel(self, kpi_day_channel_data: List[Dict]) -> None:
        if not kpi_day_channel_data:
            return
        
        now = datetime.now()
        
        data = []
        for row in kpi_day_channel_data:
            data.append([
                row['calendar_date'],
                row['year'],
                row['month'],
                row['day'],
                row['date_label'],
                row['channel'],
                row['revenue_percentage'],
                row['kpi_day_channel_initial'],
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'day', 'date_label',
            'channel', 'revenue_percentage', 'kpi_day_channel_initial',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_day_channel", data, column_names=columns)
    
    def calculate_and_save_kpi_day_channel(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        # Calculate kpi_day_channel
        kpi_day_channel_data = self.calculate_kpi_day_channel(
            target_year=target_year,
            target_month=target_month
        )
        
        self.save_kpi_day_channel(kpi_day_channel_data)
        
        return kpi_day_channel_data


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIDayChannelCalculator(constants)
    
    print("Calculating kpi_day_channel for month 1/2026...")
    kpi_day_channel_data = calculator.calculate_and_save_kpi_day_channel(
        target_year=2026,
        target_month=1
    )
    
    print(f"Successfully saved {len(kpi_day_channel_data)} kpi_day_channel records")