from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIDayCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_kpi_day_initial(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        target_version = f"Thang {target_month}"
        
        query = f"""
            SELECT 
                d.calendar_date,
                d.priority_label AS date_label,
                d.year,
                d.month,
                d.day,
                m.kpi_initial AS kpi_month,
                md.uplift,
                md.weight,
                md.total_weight_month
            FROM dim_date d
            INNER JOIN (
                SELECT 
                    year,
                    month,
                    kpi_initial,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month
                WHERE version = '{target_version}'
            ) AS m
                ON d.year = m.year 
                AND d.month = m.month
                AND m.rn = 1
            INNER JOIN (
                SELECT 
                    year,
                    month,
                    date_label,
                    uplift,
                    weight,
                    total_weight_month,
                    row_number() OVER (
                        PARTITION BY year, month, date_label
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_day_metadata
            ) AS md
                ON d.year = md.year
                AND d.month = md.month
                AND d.priority_label = md.date_label
                AND md.rn = 1
            WHERE d.year = {target_year}
              AND d.month = {target_month}
              AND NOT (
                  (d.month = 6 AND d.day = 6) OR
                  (d.month = 9 AND d.day = 9) OR
                  (d.month = 11 AND d.day = 11) OR
                  (d.month = 12 AND d.day = 12)
              )
            ORDER BY d.calendar_date
        """
        
        result = self.client.query(query)
        results = []
        
        for row in result.result_rows:
            calendar_date = row[0]
            date_label = row[1]
            year = row[2]
            month = row[3]
            day = row[4]
            kpi_month = Decimal(str(row[5]))
            uplift = Decimal(str(row[6]))
            weight = Decimal(str(row[7]))
            total_weight_month = Decimal(str(row[8]))
            
            if total_weight_month > 0:
                kpi_day_initial = (uplift * kpi_month) / total_weight_month
            else:
                kpi_day_initial = Decimal('0')
            
            results.append({
                'calendar_date': calendar_date,
                'year': year,
                'month': month,
                'day': day,
                'date_label': date_label,
                'kpi_month': float(kpi_month),
                'uplift': float(uplift),
                'weight': float(weight),
                'total_weight_month': float(total_weight_month),
                'kpi_day_initial': float(kpi_day_initial)
            })
        
        return results
    
    def save_kpi_day(self, kpi_day_data: List[Dict]) -> None:
        if not kpi_day_data:
            return
        now = datetime.now()
        
        months_needed = set()
        for row in kpi_day_data:
            months_needed.add((row['year'], row['month']))
        
        kpi_month_map = {}
        for year, month in months_needed:
            target_version = f"Thang {month}"
            kpi_month_query = f"""
                SELECT 
                    kpi_initial
                FROM (
                    SELECT 
                        year,
                        month,
                        kpi_initial,
                        row_number() OVER (
                            PARTITION BY year, month, version
                            ORDER BY updated_at DESC
                        ) AS rn
                    FROM hskcdp.kpi_month
                    WHERE year = {year}
                      AND month = {month}
                      AND version = '{target_version}'
                )
                WHERE rn = 1
                LIMIT 1
            """
            kpi_month_result = self.client.query(kpi_month_query)
            if kpi_month_result.result_rows:
                kpi_month_map[(year, month)] = float(kpi_month_result.result_rows[0][0])
        
        calendar_dates = [row['calendar_date'] for row in kpi_day_data]
        actual_map = self.revenue_helper.get_daily_actual_by_dates(calendar_dates)
        
        data = []
        for row in kpi_day_data:
            year = row['year']
            month = row['month']
            kpi_month = kpi_month_map.get((year, month), row.get('kpi_month', 0))
            
            calendar_date = row['calendar_date']
            today = date.today()
            if calendar_date <= today:
                actual = actual_map.get(calendar_date, 0)
            else:
                actual = actual_map.get(calendar_date)
            kpi_day_initial = row['kpi_day_initial']
            gap = (actual - kpi_day_initial) if actual is not None else None
            
            data.append([
                calendar_date,
                row['year'],
                row['month'],
                row['day'],
                row['date_label'],
                kpi_month,
                row['uplift'],
                row['weight'],
                row['total_weight_month'],
                row['kpi_day_initial'],
                actual,
                gap,
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'day', 'date_label',
            'kpi_month', 'uplift', 'weight', 'total_weight_month',
            'kpi_day_initial', 'actual', 'gap', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_day", data, column_names=columns)
    
    def calculate_and_save_kpi_day_initial(
        self,
        target_year: int,
        target_month: int,
    ) -> List[Dict]:
        kpi_day_data = self.calculate_kpi_day_initial(
            target_year=target_year,
            target_month=target_month
        )
        
        self.save_kpi_day(kpi_day_data)
        
        return kpi_day_data
    
    def calculate_kpi_day_adjustment(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        all_days_query = f"""
            SELECT 
                kd.calendar_date,
                kd.year,
                kd.month,
                kd.day,
                kd.date_label,
                kd.kpi_day_initial,
                kd.kpi_day_adjustment,
                kd.uplift,
                kd.weight
            FROM (SELECT * FROM hskcdp.kpi_day FINAL) AS kd
            WHERE kd.year = {target_year}
              AND kd.month = {target_month}
            ORDER BY kd.calendar_date
        """
        
        all_days_result = self.client.query(all_days_query)
        all_days = {}
        
        for row in all_days_result.result_rows:
            calendar_date = row[0]
            kpi_day_adjustment = row[6] if row[6] is not None else None
            all_days[calendar_date] = {
                'year': row[1],
                'month': row[2],
                'day': row[3],
                'date_label': row[4],
                'kpi_day_initial': Decimal(str(row[5])),
                'kpi_day_adjustment': Decimal(str(kpi_day_adjustment)) if kpi_day_adjustment is not None else None,
                'uplift': Decimal(str(row[7])),
                'weight': Decimal(str(row[8]))
            }
        
        actuals_dict = self.revenue_helper.get_daily_actual_by_month(target_year, target_month)
        actuals = {date: Decimal(str(amount)) for date, amount in actuals_dict.items()}
        
        days_with_actual = set()
        total_gap = Decimal('0')
        today = date.today()
        
        eod_value = None
        current_datetime = datetime.now()
        current_hour = current_datetime.hour
        
        if today.year == target_year and today.month == target_month and today in all_days:
            print(f"\n=== DEBUG: Calculating EOD for date {today} ===")
            print(f"DEBUG: Current datetime = {current_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"DEBUG: Current hour (rounded down) = {current_hour}h")
            print(f"DEBUG: Get actual from 00:00 to <{current_hour}h (i.e., from 00:00 to {current_hour - 1}h59)")
            
            hourly_percentages = self.revenue_helper.get_hourly_revenue_percentage(days_back=30)
            print(f"DEBUG: Hourly revenue percentages (30 recent days):")
            for hour in range(24):
                if hour in hourly_percentages:
                    percentage = hourly_percentages[hour]
                    print(f"  - Hour {hour:2d}h: {percentage:.6f} ({percentage * 100:.4f}%)")
            
            total_percentage_passed = Decimal('0')
            print(f"\nDEBUG: Calculate total % of hours passed (0h to {current_hour - 1}h):")
            for hour in range(current_hour):
                if hour in hourly_percentages:
                    percentage = Decimal(str(hourly_percentages[hour]))
                    total_percentage_passed += percentage
                    print(f"  - Hour {hour:2d}h: {percentage:.6f} ({float(percentage) * 100:.4f}%)")
            
            print(f"DEBUG: Total % of hours passed = {total_percentage_passed} ({float(total_percentage_passed) * 100:.4f}%)")
            
            actual_until_hour = self.revenue_helper.get_daily_actual_until_hour(today, current_hour)
            print(f"DEBUG: Actual from 00:00 to <{current_hour}h (i.e., from 00:00 to {current_hour - 1}h59) = {actual_until_hour}")
            
            if total_percentage_passed > 0 and actual_until_hour > 0:
                eod_value = float(actual_until_hour / total_percentage_passed)
            else:
                if total_percentage_passed == 0:
                    print(f"DEBUG: Reason: Total % of hours passed = 0 (no data)")
                if actual_until_hour == 0:
                    print(f"DEBUG: Reason: Actual from start of day = 0 (no revenue yet)")
        else:
            print(f"DEBUG: Current date ({today}) is not in the target month ({target_month}/{target_year})")
        
        for calendar_date, actual_amount in actuals.items():
            if calendar_date in all_days:
                if calendar_date == today:
                    continue
                kpi_day_initial = all_days[calendar_date]['kpi_day_initial']
                gap = actual_amount - kpi_day_initial
                total_gap += gap
                days_with_actual.add(calendar_date)
        
        if today in all_days:
            kpi_day_initial_today = all_days[today]['kpi_day_initial']
            if eod_value is not None:
                gap_today = Decimal(str(eod_value)) - kpi_day_initial_today
                total_gap += gap_today
                days_with_actual.add(today)
                print(f"DEBUG: Gap of today (from EOD) = {gap_today}")
            else:
                if today in actuals:
                    gap_today = actuals[today] - kpi_day_initial_today
                    total_gap += gap_today
                    days_with_actual.add(today)
                else:
                    gap_today = Decimal('0') - kpi_day_initial_today
                    total_gap += gap_today
        
        for calendar_date, day_data in all_days.items():
            if calendar_date not in days_with_actual and calendar_date < today:
                kpi_day_initial = day_data['kpi_day_initial']
                gap = Decimal('0') - kpi_day_initial
                total_gap += gap
        
        total_weight_left = Decimal('0')
        days_in_weighted_left = []
        for calendar_date, day_data in all_days.items():
            if calendar_date not in days_with_actual and calendar_date > today:
                uplift = day_data['uplift']
                total_weight_left += uplift
                days_in_weighted_left.append((calendar_date, uplift))
        
        avg_rev_normal_day = None
        normal_day_metadata_query = f"""
            SELECT 
                avg_total
            FROM hskcdp.kpi_day_metadata
            WHERE year = {target_year}
                AND month = {target_month}
                AND date_label = 'Normal day'
            ORDER BY updated_at DESC
            LIMIT 1
        """
        normal_day_result = self.client.query(normal_day_metadata_query)
        if normal_day_result.result_rows and normal_day_result.result_rows[0][0] is not None:
            avg_rev_normal_day = Decimal(str(normal_day_result.result_rows[0][0]))
        
        results = []
        for calendar_date, day_data in all_days.items():
            kpi_day_initial = day_data['kpi_day_initial']
            weight = day_data['weight']
            
            if calendar_date in actuals:
                actual_amount = actuals[calendar_date]
                kpi_day_adjustment = actual_amount
                actual_amount_value = float(actual_amount)
                gap = kpi_day_adjustment - kpi_day_initial
                weighted_left = Decimal('0')
            else:
                actual_amount_value = None
                uplift = day_data['uplift']
                
                if calendar_date <= today:
                    kpi_day_adjustment = Decimal('0')
                    gap = Decimal('0') - kpi_day_initial
                    weighted_left = Decimal('0')
                else:
                    weighted_left = uplift
                    if total_weight_left > 0:
                        gap_portion = (total_gap * uplift) / total_weight_left
                        kpi_day_adjustment = kpi_day_initial - gap_portion
                    else:
                        kpi_day_adjustment = kpi_day_initial
                    
                    gap = None
            
            if calendar_date == today:
                eod = eod_value
                kpi_day_adjustment = eod_value
                if eod_value is not None:
                    gap = Decimal(str(eod_value)) - kpi_day_initial
                else:
                    gap = None
            elif calendar_date < today:
                if calendar_date in actuals:
                    eod = float(actuals[calendar_date])
                else:
                    eod = 0.0
            else:
                if avg_rev_normal_day is not None:
                    uplift = day_data['uplift']
                    eod = float(avg_rev_normal_day * uplift)
                else:
                    eod = None
            
            results.append({
                'calendar_date': calendar_date,
                'year': day_data['year'],
                'month': day_data['month'],
                'day': day_data['day'],
                'date_label': day_data['date_label'],
                'kpi_day_initial': float(kpi_day_initial),
                'uplift': float(day_data['uplift']),
                'weight': float(weight),
                'weighted_left': float(weighted_left),
                'actual_amount': actual_amount_value,
                'gap': float(gap) if gap is not None else None,
                'kpi_day_adjustment': float(kpi_day_adjustment),
                'eod': eod
            })
        
        return results
    
    def update_kpi_day_adjustment(self, kpi_day_adjustment_data: List[Dict]) -> None:
        if not kpi_day_adjustment_data:
            return
        
        now = datetime.now()
        
        months_needed = set()
        for row in kpi_day_adjustment_data:
            months_needed.add((row['year'], row['month']))
        
        kpi_month_map = {}
        for year, month in months_needed:
            target_version = f"Thang {month}"
            kpi_month_query = f"""
                SELECT 
                    kpi_initial
                FROM (
                    SELECT 
                        year,
                        month,
                        kpi_initial,
                        row_number() OVER (
                            PARTITION BY year, month, version
                            ORDER BY updated_at DESC
                        ) AS rn
                    FROM hskcdp.kpi_month
                    WHERE year = {year}
                      AND month = {month}
                      AND version = '{target_version}'
                )
                WHERE rn = 1
                LIMIT 1
            """
            kpi_month_result = self.client.query(kpi_month_query)
            if kpi_month_result.result_rows:
                kpi_month_map[(year, month)] = float(kpi_month_result.result_rows[0][0])
        
        calendar_dates = [row['calendar_date'] for row in kpi_day_adjustment_data]
        dates_str = ','.join([f"'{cd}'" for cd in calendar_dates])
        
        get_current_query = f"""
            SELECT 
                calendar_date,
                year,
                month,
                day,
                date_label,
                uplift,
                weight,
                total_weight_month,
                kpi_day_initial
            FROM hskcdp.kpi_day FINAL
            WHERE calendar_date IN ({dates_str})
        """
        
        current_result = self.client.query(get_current_query)
        current_data_map = {}
        for row in current_result.result_rows:
            calendar_date = row[0]
            current_data_map[calendar_date] = {
                'year': row[1],
                'month': row[2],
                'day': row[3],
                'date_label': row[4],
                'uplift': float(row[5]),
                'weight': float(row[6]),
                'total_weight_month': float(row[7]),
                'kpi_day_initial': float(row[8])
            }
        
        actual_map = self.revenue_helper.get_daily_actual_by_dates(calendar_dates)
        
        data = []
        for row in kpi_day_adjustment_data:
            calendar_date = row['calendar_date']
            current_data = current_data_map.get(calendar_date, {})
            
            year = row['year']
            month = row['month']
            kpi_month = kpi_month_map.get((year, month), 0)
            
            kpi_day_initial_raw = current_data.get('kpi_day_initial', row['kpi_day_initial'])
            kpi_day_initial = Decimal(str(kpi_day_initial_raw))
            uplift = current_data.get('uplift', row.get('uplift', 0))
            today = date.today()
            
            if calendar_date <= today:
                actual_raw = actual_map.get(calendar_date, 0)
                actual = Decimal(str(actual_raw)) if actual_raw is not None else None
            else:
                actual_raw = actual_map.get(calendar_date)
                actual = Decimal(str(actual_raw)) if actual_raw is not None else None
            
            eod_value = row.get('eod')
            
            if calendar_date == today:
                if eod_value is not None:
                    gap = Decimal(str(eod_value)) - kpi_day_initial
                    weighted_left = 0
                else:
                    gap = None
                    weighted_left = 0
            elif actual is not None and actual != 0:
                weighted_left = 0
                gap = actual - kpi_day_initial
            elif calendar_date < today:
                weighted_left = 0
                gap = Decimal('0') - kpi_day_initial
            else:
                weighted_left = uplift
                gap = None
            
            data.append([
                calendar_date,
                row['year'],
                row['month'],
                row['day'],
                row['date_label'],
                kpi_month,
                current_data.get('uplift', row.get('uplift', 0)),
                current_data.get('weight', 0),
                current_data.get('total_weight_month', 0),
                current_data.get('kpi_day_initial', row['kpi_day_initial']),
                float(actual) if actual is not None else None,
                float(gap) if gap is not None else None,
                row['kpi_day_adjustment'],
                weighted_left,
                eod_value,
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'day', 'date_label',
            'kpi_month', 'uplift', 'weight', 'total_weight_month',
            'kpi_day_initial', 'actual', 'gap', 'kpi_day_adjustment', 'weighted_left', 'eod', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_day", data, column_names=columns)
    
    def calculate_and_save_kpi_day_adjustment(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        kpi_day_adjustment_data = self.calculate_kpi_day_adjustment(
            target_year=target_year,
            target_month=target_month
        )
        
        self.update_kpi_day_adjustment(kpi_day_adjustment_data)
        
        return kpi_day_adjustment_data


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPIDayCalculator(constants)
    
    target_month = None
    target_year = constants.KPI_YEAR_2026
    
    if len(sys.argv) > 1:
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--target-month":
                if i + 1 < len(sys.argv):
                    try:
                        target_month = int(sys.argv[i + 1])
                        i += 2
                    except ValueError:
                        print(f"Error: Invalid value for --target-month: {sys.argv[i + 1]}")
                        sys.exit(1)
                else:
                    print("Error: --target-month requires a value")
                    sys.exit(1)
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
    
    print(f"Calculating kpi_day_initial for month {target_month}/{target_year}...")
    kpi_day_initial_data = calculator.calculate_and_save_kpi_day_initial(
        target_year=target_year,
        target_month=target_month
    )
    print(f"Successfully saved {len(kpi_day_initial_data)} kpi_day_initial records")
    
    print(f"Calculating kpi_day_adjustment for month {target_month}/{target_year}...")
    kpi_day_adjustment_data = calculator.calculate_and_save_kpi_day_adjustment(
        target_year=target_year,
        target_month=target_month
    )
    print(f"Successfully saved {len(kpi_day_adjustment_data)} kpi_day_adjustment records")