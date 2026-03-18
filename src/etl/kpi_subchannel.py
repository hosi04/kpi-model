from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPISubChannelCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_and_save_kpi_subchannel(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        
        # Get kpi_channel_initial and rev_pct 
        kpi_subchannel_metadata_data = self.revenue_helper.get_kpi_channel_with_subchannel_metadata(
            target_year=target_year,
            target_month=target_month
        )
        
        # Get actual revenue by subchannel, channel and date
        actual_by_date = self.revenue_helper.get_actual_by_subchannel_channel_and_date(
            target_year=target_year,
            target_month=target_month
        )
        
        results = []
        data_to_insert = []
        now = datetime.now()
        today = date.today()
        
        for row in kpi_subchannel_metadata_data:
            calendar_date = row['calendar_date']
            year = row['year']
            month = row['month']
            day = row['day']
            date_label = row['date_label']
            channel = row['channel']
            subchannel = row['subchannel']
            store_name = row['store_name']
            rev_pct = row['rev_pct']
            kpi_channel_initial = row['kpi_channel_initial']
            kpi_channel_adjustment = row['kpi_channel_adjustment']
            
            # Tính kpi_subchannel_initial
            kpi_subchannel_initial = kpi_channel_initial * rev_pct
            
            # Lấy actual của store_name (trong subchannel) nếu có
            actual = actual_by_date.get(calendar_date, {}).get(channel, {}).get(subchannel, {}).get(store_name, 0.0)       
            
            if calendar_date < today:
                kpi_subchannel_adjustment = Decimal(str(actual))
                gap = Decimal(str(actual)) - kpi_subchannel_initial
            else:
                gap = Decimal('0')
                if kpi_channel_adjustment is not None:
                    kpi_subchannel_adjustment = kpi_channel_adjustment * rev_pct
                else:
                    kpi_subchannel_adjustment = None
            
            actual_decimal = Decimal(str(actual)) if actual is not None else None
            gap_decimal = Decimal(str(gap)) if gap is not None else None
            
            results.append({
                'calendar_date': calendar_date,
                'year': year,
                'month': month,
                'day': day,
                'date_label': date_label,
                'channel': channel,
                'subchannel': subchannel,
                'store_name': store_name,
                'rev_pct': rev_pct,
                'kpi_subchannel_initial': kpi_subchannel_initial,
                'actual': actual_decimal,
                'gap': gap_decimal,
                'kpi_subchannel_adjustment': kpi_subchannel_adjustment if kpi_subchannel_adjustment is not None else None
            })
            
            data_to_insert.append([
                calendar_date,
                year,
                month,
                day,
                date_label,
                channel,
                subchannel,
                store_name,
                rev_pct,
                kpi_subchannel_initial,
                actual_decimal,
                gap_decimal,
                kpi_subchannel_adjustment,
                now,
                now
            ])
        
        if data_to_insert:
            columns = [
                'calendar_date', 'year', 'month', 'day', 'date_label',
                'channel', 'subchannel', 'store_name', 'rev_pct', 
                'kpi_subchannel_initial', 'actual', 'gap', 
                'kpi_subchannel_adjustment', 'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_subchannel", data_to_insert, column_names=columns)
            
        return results


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPISubChannelCalculator(constants)
    
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
        # Chú ý: khác với kpi_metadata, kpi ngày chạy cho tháng hiện tại
        if today.year == constants.KPI_YEAR_2026:
            target_month = today.month
        else:
            target_month = 1
    
    if target_month < 1 or target_month > 12:
        print(f"Error: target_month must be between 1 and 12, received: {target_month}")
        sys.exit(1)
    
    print(f"Calculating kpi_subchannel for month {target_month}/{target_year}...")
    kpi_subchannel_data = calculator.calculate_and_save_kpi_subchannel(
        target_year=target_year,
        target_month=target_month
    )
    
    print(f"Successfully saved {len(kpi_subchannel_data)} kpi_subchannel records")
