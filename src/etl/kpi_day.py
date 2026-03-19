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
    
    def calculate_and_save_kpi_day_initial(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        """
        Calculates initial KPI for each day and saves to ClickHouse.
        Merged calculation and saving into one function.
        """
        target_version = f"Thang {target_month}"
        now = datetime.now()
        
        # 1. Fetch data for calculation
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
            FROM hskcdp.dim_date AS d
            INNER JOIN (
                SELECT 
                    year,
                    month,
                    kpi_initial
                FROM hskcdp.kpi_month FINAL
                WHERE year = {target_year}
                  AND version = '{target_version}'
            ) AS m
                ON d.year = m.year 
                AND d.month = m.month
            INNER JOIN (
                SELECT 
                    year,
                    month,
                    date_label,
                    uplift,
                    weight,
                    total_weight_month
                FROM hskcdp.kpi_day_metadata FINAL
            ) AS md
                ON d.year = md.year
                AND d.month = md.month
                AND d.priority_label = md.date_label
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
        
        # 2. Get actual revenue data for GAP calculation
        calendar_dates = [row[0] for row in result.result_rows]
        actual_map = self.revenue_helper.get_daily_actual_by_dates(calendar_dates) if calendar_dates else {}
        
        results = []
        data_to_insert = []
        today = date.today()
        
        # 3. Process and prepare data for insertion
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
            
            # KPI Day Initial Calculation
            if total_weight_month > 0:
                kpi_day_initial = (uplift * kpi_month) / total_weight_month
            else:
                kpi_day_initial = Decimal('0')
            
            # Actual and Gap Calculation
            if calendar_date <= today:
                actual = actual_map.get(calendar_date, 0)
            else:
                actual = actual_map.get(calendar_date)
            
            gap = (actual - kpi_day_initial) if actual is not None else None
            
            # Store in results list for returning
            results.append({
                'calendar_date': calendar_date,
                'year': year,
                'month': month,
                'day': day,
                'date_label': date_label,
                'kpi_month': kpi_month,
                'uplift': uplift,
                'weight': weight,
                'total_weight_month': total_weight_month,
                'kpi_day_initial': kpi_day_initial,
                'actual': actual,
                'gap': gap
            })
            
            # Prepare row for ClickHouse insert
            data_to_insert.append([
                calendar_date, year, month, day, date_label,
                float(kpi_month), float(uplift), float(weight), float(total_weight_month),
                float(kpi_day_initial), 
                float(actual) if actual is not None else None,
                float(gap) if gap is not None else None,
                now, now
            ])
        
        # 4. Save to ClickHouse
        if data_to_insert:
            columns = [
                'calendar_date', 'year', 'month', 'day', 'date_label',
                'kpi_month', 'uplift', 'weight', 'total_weight_month',
                'kpi_day_initial', 'actual', 'gap', 'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_day", data_to_insert, column_names=columns)
            print(f"[INFO] Successfully inserted {len(data_to_insert)} initial records into kpi_day")
        
        return results

    def calculate_and_save_kpi_day_adjustment(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        """
        Calculates adjusted KPI for each day based on actual performance and saves to ClickHouse.
        Merged calculation and saving into one function.
        """
        now = datetime.now()
        today = date.today()
        target_version = f"Thang {target_month}"
        
        # 1. Fetch current kpi_day data (base for adjustment)
        query = f"""
            SELECT 
                calendar_date, year, month, day, date_label,
                kpi_month, uplift, weight, total_weight_month, kpi_day_initial
            FROM hskcdp.kpi_day FINAL
            WHERE year = {target_year} AND month = {target_month}
            ORDER BY calendar_date
        """
        all_days_result = self.client.query(query)
        
        if not all_days_result.result_rows:
            print(f"[WARNING] No initial kpi_day data found for {target_month}/{target_year}")
            return []

        all_days = {}
        for row in all_days_result.result_rows:
            cal_date = row[0]
            all_days[cal_date] = {
                'year': row[1], 'month': row[2], 'day': row[3], 'date_label': row[4],
                'kpi_month': Decimal(str(row[5])),
                'uplift': Decimal(str(row[6])),
                'weight': Decimal(str(row[7])),
                'total_weight_month': Decimal(str(row[8])),
                'kpi_day_initial': Decimal(str(row[9]))
            }

        # 2. Get Actuals, Forecast and EOD data
        actuals_dict = self.revenue_helper.get_daily_actual_by_month(target_year, target_month)
        forecast_by_day = self.revenue_helper.get_forecast_by_day(target_year, target_month)
        actuals = {d: Decimal(str(amt)) for d, amt in actuals_dict.items()}
        
        # EOD Calculation for today
        eod_value = None
        if today in all_days:
            current_hour = now.hour
            hourly_percentages = self.revenue_helper.get_hourly_revenue_percentage(days_back=30)
            total_percentage_passed = sum(Decimal(str(hourly_percentages.get(h, 0))) for h in range(current_hour))
            actual_until_hour = self.revenue_helper.get_daily_actual_until_hour(today, current_hour)
            
            if total_percentage_passed > 0 and actual_until_hour > 0:
                eod_value = float(actual_until_hour / total_percentage_passed)

        # 3. GAP Distribution Logic
        days_with_actual = set()
        total_gap = Decimal('0')
        
        for calendar_date, actual_amount in actuals.items():
            if calendar_date in all_days:
                if calendar_date == today: continue
                total_gap += (actual_amount - all_days[calendar_date]['kpi_day_initial'])
                days_with_actual.add(calendar_date)
        
        if today in all_days:
            gap_today = forecast_by_day.get(today, Decimal('0')) - all_days[today]['kpi_day_initial']
            total_gap += gap_today
            days_with_actual.add(today)
            
        for calendar_date, day_data in all_days.items():
            if calendar_date not in days_with_actual and calendar_date < today:
                total_gap -= day_data['kpi_day_initial']
        
        total_weight_left = sum(d['uplift'] for cd, d in all_days.items() if cd not in days_with_actual and cd > today)
        
        # Get Avg Revenue Normal day for EOD estimation of future days
        avg_rev_normal_day = None
        normal_day_result = self.client.query(f"SELECT avg_total FROM hskcdp.kpi_day_metadata WHERE year={target_year} AND month={target_month} AND date_label='Normal day' ORDER BY updated_at DESC LIMIT 1")
        if normal_day_result.result_rows and normal_day_result.result_rows[0][0]:
            avg_rev_normal_day = Decimal(str(normal_day_result.result_rows[0][0]))

        # 4. Prepare Adjustment Data
        results = []
        data_to_insert = []
        
        for calendar_date, day_data in all_days.items():
            kpi_day_initial = day_data['kpi_day_initial']
            uplift = day_data['uplift']
            
            # Calculate Adjustment, Gap, Weighted Left, EOD
            actual = actuals.get(calendar_date)
            weighted_left = Decimal('0')
            gap = None
            eod = None
            
            if calendar_date == today:
                eod = Decimal(forecast_by_day.get(calendar_date, 0))
                kpi_day_adjustment = Decimal(str(eod))
                gap = kpi_day_adjustment - kpi_day_initial
            elif calendar_date in actuals:
                kpi_day_adjustment = actual
                gap = kpi_day_adjustment - kpi_day_initial
                eod = float(actual)
            elif calendar_date < today:
                kpi_day_adjustment = Decimal('0')
                gap = Decimal('0') - kpi_day_initial
                eod = 0.0
            else:
                weighted_left = uplift
                if total_weight_left > 0:
                    gap_portion = (total_gap * uplift) / total_weight_left
                    kpi_day_adjustment = kpi_day_initial - gap_portion
                else:
                    kpi_day_adjustment = kpi_day_initial
                
                if avg_rev_normal_day:
                    eod = float(avg_rev_normal_day * uplift)

            results.append({
                'calendar_date': calendar_date,
                **{k: v for k, v in day_data.items() if k != 'kpi_day_initial'},
                'kpi_day_initial': float(kpi_day_initial),
                'kpi_day_adjustment': float(kpi_day_adjustment),
                'actual': float(actual) if actual is not None else None,
                'gap': float(gap) if gap is not None else None,
                'weighted_left': float(weighted_left),
                'eod': eod
            })
            
            data_to_insert.append([
                calendar_date, day_data['year'], day_data['month'], day_data['day'], day_data['date_label'],
                float(day_data['kpi_month']), float(uplift), float(day_data['weight']), float(day_data['total_weight_month']),
                float(kpi_day_initial), float(actual) if actual is not None else None, 
                float(gap) if gap is not None else None, float(kpi_day_adjustment), float(weighted_left), 
                eod, now, now
            ])

        # 5. Save to ClickHouse
        if data_to_insert:
            columns = [
                'calendar_date', 'year', 'month', 'day', 'date_label',
                'kpi_month', 'uplift', 'weight', 'total_weight_month',
                'kpi_day_initial', 'actual', 'gap', 'kpi_day_adjustment', 'weighted_left', 'eod', 'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_day", data_to_insert, column_names=columns)
            print(f"[INFO] Successfully inserted {len(data_to_insert)} adjusted records into kpi_day")

        return results


if __name__ == "__main__":
    import sys
    constants = Constants()
    calculator = KPIDayCalculator(constants)
    
    target_month = None
    target_year = constants.KPI_YEAR_2026
    
    if len(sys.argv) > 1:
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--target-month" and i + 1 < len(sys.argv):
                target_month = int(sys.argv[i+1]); i += 2
            else: i += 1
    
    if target_month is None:
        today = date.today()
        target_month = today.month if today.year == constants.KPI_YEAR_2026 else 1
    
    print(f"Processing kpi_day for month {target_month}/{target_year}...")
    calculator.calculate_and_save_kpi_day_initial(target_year, target_month)
    calculator.calculate_and_save_kpi_day_adjustment(target_year, target_month)
