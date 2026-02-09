from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIDayChannelCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_kpi_day_channel(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        # Sử dụng helper method để query từ kpi_day và kpi_channel_metadata
        kpi_day_channel_data = self.revenue_helper.get_kpi_day_with_channel_metadata(
            target_year=target_year,
            target_month=target_month
        )
        
        # Lấy actual revenue theo channel và date
        actual_by_date = self.revenue_helper.get_actual_by_channel_and_date(
            target_year=target_year,
            target_month=target_month
        )
        
        # Lấy kpi_day_adjustment theo date
        kpi_day_adjustment_by_date = self.revenue_helper.get_kpi_day_adjustment_by_date(
            target_year=target_year,
            target_month=target_month
        )
        
        results = []
        today = date.today()
        
        for row in kpi_day_channel_data:
            calendar_date = row['calendar_date']
            year = row['year']
            month = row['month']
            day = row['day']
            date_label = row['date_label']
            channel = row['channel']
            rev_pct_adjustment = row['rev_pct_adjustment']
            kpi_day_initial = row['kpi_day_initial']
            
            # Calculate kpi_day_channel_initial using rev_pct_adjustment
            kpi_day_channel_initial = kpi_day_initial * rev_pct_adjustment
            
            # Lấy actual revenue cho channel này trong ngày này
            actual = actual_by_date.get(calendar_date, {}).get(channel, 0.0)       
            
            if calendar_date <= today:
                kpi_adjustment = actual
                gap = actual - float(kpi_day_channel_initial)
            else:
                gap = 0
                kpi_day_adjustment = kpi_day_adjustment_by_date.get(calendar_date)
                if kpi_day_adjustment is not None:
                    kpi_adjustment = float(kpi_day_adjustment) * float(rev_pct_adjustment)
                else:
                    kpi_adjustment = None
            
            results.append({
                'calendar_date': calendar_date,
                'year': year,
                'month': month,
                'day': day,
                'date_label': date_label,
                'channel': channel,
                'rev_pct': float(rev_pct_adjustment),
                'kpi_channel_initial': float(kpi_day_channel_initial),
                'actual': actual,
                'gap': gap,
                'kpi_channel_adjustment': kpi_adjustment
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
                row['rev_pct'],
                row['kpi_channel_initial'],
                row['actual'],
                row['gap'],
                row['kpi_channel_adjustment'],
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'day', 'date_label',
            'channel', 'rev_pct', 'kpi_channel_initial',
            'actual', 'gap', 'kpi_channel_adjustment',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_channel", data, column_names=columns)
    
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