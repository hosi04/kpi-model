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
        kpi_day_channel_data = self.revenue_helper.get_kpi_day_with_channel_metadata(
            target_year=target_year,
            target_month=target_month
        )
        
        actual_by_date = self.revenue_helper.get_actual_by_channel_and_date(
            target_year=target_year,
            target_month=target_month
        )
        
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
            
            # Calculate kpi_channel_initial using rev_pct_adjustment
            kpi_channel_initial = kpi_day_initial * rev_pct_adjustment
            
            # Get actual revenue for this channel on this date
            actual = actual_by_date.get(calendar_date, {}).get(channel, 0.0)       
            
            if calendar_date < today:
                kpi_channel_adjustment = Decimal(str(actual))
                gap = Decimal(str(actual)) - kpi_channel_initial
            else:
                gap = Decimal('0')
                kpi_day_adjustment = kpi_day_adjustment_by_date.get(calendar_date)
                if kpi_day_adjustment is not None:
                    kpi_channel_adjustment = kpi_day_adjustment * rev_pct_adjustment
                else:
                    kpi_channel_adjustment = None
            
            results.append({
                'calendar_date': calendar_date,
                'year': year,
                'month': month,
                'day': day,
                'date_label': date_label,
                'channel': channel,
                'rev_pct': rev_pct_adjustment,
                'kpi_channel_initial': kpi_channel_initial,
                'actual': Decimal(str(actual)) if actual is not None else None,
                'gap': Decimal(str(gap)) if gap is not None else None,
                'kpi_channel_adjustment': kpi_channel_adjustment if kpi_channel_adjustment is not None else None
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
    import sys
    
    constants = Constants()
    calculator = KPIDayChannelCalculator(constants)
    
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
    
    print(f"Calculating kpi_day_channel for month {target_month}/{target_year}...")
    kpi_day_channel_data = calculator.calculate_and_save_kpi_day_channel(
        target_year=target_year,
        target_month=target_month
    )
    
    print(f"Successfully saved {len(kpi_day_channel_data)} kpi_day_channel records")