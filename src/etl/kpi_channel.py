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
    
    def calculate_and_save_kpi_day_channel(
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
        forecast_by_channel_for_today = self.revenue_helper.get_forecast_by_channel_for_today()
        
        forecast_top_down = self.revenue_helper.get_forecast_top_down_from_day(
            target_year=target_year, 
            target_month=target_month
        )

        results = []
        data_to_insert = []
        today = date.today()
        now = datetime.now()

        channel_groups = {}
        for row in kpi_day_channel_data:
            ch = row['channel']
            if ch not in channel_groups:
                channel_groups[ch] = []
            channel_groups[ch].append(row)

        for channel, rows in channel_groups.items():
            total_gap_channel = Decimal('0')
            total_weight_left_channel = Decimal('0')
            
            for row in rows:
                cal_date = row['calendar_date']
                kpi_init = row['kpi_day_initial'] * row['rev_pct_adjustment']
                actual = Decimal(str(actual_by_date.get(cal_date, {}).get(channel, 0.0)))
                
                if cal_date < today:
                    total_gap_channel += (actual - kpi_init)
                elif cal_date == today:
                    forecast_today = forecast_by_channel_for_today.get(channel, Decimal('0'))
                    total_gap_channel += (forecast_today - kpi_init)
                else:
                    total_weight_left_channel += row['rev_pct_adjustment']
            for row in rows:
                calendar_date = row['calendar_date']
                rev_pct_adjustment = row['rev_pct_adjustment']
                kpi_channel_initial = row['kpi_day_initial'] * rev_pct_adjustment
                actual = Decimal(str(actual_by_date.get(calendar_date, {}).get(channel, 0.0)))
                
                if calendar_date < today:
                    kpi_channel_adjustment = actual
                    gap = actual - kpi_channel_initial
                elif calendar_date == today:
                    kpi_channel_adjustment = forecast_by_channel_for_today.get(channel, Decimal('0'))
                    gap = kpi_channel_adjustment - kpi_channel_initial
                else:
                    if total_weight_left_channel > 0:
                        gap_portion = (total_gap_channel * rev_pct_adjustment) / total_weight_left_channel
                        kpi_channel_adjustment = kpi_channel_initial - gap_portion
                    else:
                        kpi_channel_adjustment = kpi_channel_initial
                    gap = Decimal('0')

                # Forecast
                forecast = None 
                if calendar_date < today:
                    forecast = actual 
                elif calendar_date == today:
                    # forecast bottom-up
                    forecast = forecast_by_channel_for_today.get(channel, Decimal('0'))
                else:
                    # forecast top-down
                    forecast = forecast_top_down.get(str(calendar_date), Decimal('0')) * rev_pct_adjustment

                actual_decimal = Decimal(str(actual)) if actual is not None else None
                gap_decimal = Decimal(str(gap)) if gap is not None else None

                results.append({
                    'calendar_date': calendar_date,
                    'year': row['year'],
                    'month': row['month'],
                    'day': row['day'],
                    'date_label': row['date_label'],
                    'channel': channel,
                    'rev_pct': rev_pct_adjustment,
                    'kpi_channel_initial': kpi_channel_initial,
                    'actual': actual_decimal,
                    'gap': gap_decimal,
                    'kpi_channel_adjustment': kpi_channel_adjustment,
                    'forecast': forecast
                })
                
                data_to_insert.append([
                    calendar_date,
                    row['year'],
                    row['month'],
                    row['day'],
                    row['date_label'],
                    channel,
                    rev_pct_adjustment,
                    kpi_channel_initial,
                    actual_decimal,
                    gap_decimal,
                    kpi_channel_adjustment,
                    forecast,
                    now,
                    now
                ])
        
        if data_to_insert:
            columns = [
                'calendar_date', 'year', 'month', 'day', 'date_label',
                'channel', 'rev_pct', 'kpi_channel_initial',
                'actual', 'gap', 'kpi_channel_adjustment', 'forecast',
                'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_channel", data_to_insert, column_names=columns)

        return results


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