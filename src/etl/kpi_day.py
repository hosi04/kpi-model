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
        # Lấy version tương ứng với tháng đó (ví dụ: tháng 1 → version "Thang 1")
        target_version = f"Thang {target_month}"
        
        query = f"""
            SELECT 
                d.calendar_date,
                d.date_label,
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
                AND d.date_label = md.date_label
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
        
        # Lấy danh sách các tháng cần lấy kpi_month mới nhất
        months_needed = set()
        for row in kpi_day_data:
            months_needed.add((row['year'], row['month']))
        
        # Lấy kpi_month.kpi_initial với version tương ứng với tháng đó
        # Tháng 1 → version "Thang 1", Tháng 2 → version "Thang 2", ...
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
        
        # Lấy actual từ transactions cho tất cả các ngày
        calendar_dates = [row['calendar_date'] for row in kpi_day_data]
        actual_map = self.revenue_helper.get_daily_actual_by_dates(calendar_dates)
        
        data = []
        for row in kpi_day_data:
            # Lấy kpi_month mới nhất từ kpi_month_map (từ bảng kpi_month)
            year = row['year']
            month = row['month']
            kpi_month = kpi_month_map.get((year, month), row.get('kpi_month', 0))
            
            # Lấy actual và tính gap
            calendar_date = row['calendar_date']
            today = date.today()
            # Chỉ set actual = 0 cho ngày hiện tại và ngày đã qua
            # Ngày chưa tới thì actual = None
            if calendar_date <= today:
                actual = actual_map.get(calendar_date, 0)  # Nếu không có actual thì = 0
            else:
                actual = actual_map.get(calendar_date)  # Ngày chưa tới: None nếu không có
            kpi_day_initial = row['kpi_day_initial']
            gap = (actual - kpi_day_initial) if actual is not None else None
            
            data.append([
                calendar_date,
                row['year'],
                row['month'],
                row['day'],
                row['date_label'],
                kpi_month,  # Lấy từ kpi_month mới nhất, không dùng row['kpi_month']
                row['uplift'],
                row['weight'],
                row['total_weight_month'],
                row['kpi_day_initial'],
                actual,  # actual từ actual_2026_day_staging
                gap,     # gap = actual - kpi_day_initial
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
        """
        Tính kpi_day_adjustment cho TẤT CẢ các ngày trong tháng:
        - Ngày có actual: kpi_day_adjustment = actual
        - Ngày không có actual: kpi_day_adjustment = kpi_day_initial + (Gap * uplift / SUM(uplift của các ngày chưa có actual))
        """
        # Lấy tất cả các ngày trong tháng với kpi_day_initial, kpi_day_adjustment (nếu có), uplift và weight
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
        
        # Lấy actual của các ngày có actual từ transactions
        actuals_dict = self.revenue_helper.get_daily_actual_by_month(target_year, target_month)
        actuals = {date: Decimal(str(amount)) for date, amount in actuals_dict.items()}
        
        # Tính tổng gap của TẤT CẢ các ngày có actual VÀ các ngày đã qua không có actual
        # Logic: Mỗi lần chạy sẽ tính lại gap của tất cả các ngày có actual
        # Gap của mỗi ngày có actual = actual - kpi_day_initial
        # Gap của mỗi ngày đã qua và không có actual = 0 - kpi_day_initial = -kpi_day_initial
        # SUM(Gap) = tổng gap của tất cả các ngày có actual + tổng gap của các ngày đã qua và không có actual
        # Mục đích: để SUM(kpi_day_adjustment) = SUM(kpi_day_initial)
        days_with_actual = set()
        total_gap = Decimal('0')
        today = date.today()
        
        print(f"\n=== DEBUG: Tính SUM(Gap) ===")
        # Tính gap từ các ngày có actual
        for calendar_date, actual_amount in actuals.items():
            if calendar_date in all_days:
                # Tính gap cho tất cả các ngày có actual (không chỉ các ngày có actual mới)
                kpi_day_initial = all_days[calendar_date]['kpi_day_initial']
                gap = actual_amount - kpi_day_initial
                total_gap += gap
                days_with_actual.add(calendar_date)
                print(f"DEBUG: Ngày {calendar_date}: actual={actual_amount}, kpi_day_initial={kpi_day_initial}, gap={gap}")
        
        # Tính gap từ các ngày đã qua và không có actual (gap = -kpi_day_initial)
        for calendar_date, day_data in all_days.items():
            if calendar_date not in days_with_actual and calendar_date <= today:
                kpi_day_initial = day_data['kpi_day_initial']
                gap = Decimal('0') - kpi_day_initial  # gap = -kpi_day_initial
                total_gap += gap
                print(f"DEBUG: Ngày {calendar_date} (đã qua, không có actual): kpi_day_initial={kpi_day_initial}, gap={gap}")
        
        print(f"DEBUG: SUM(Gap) = {total_gap}")
        
        # Tính kpi_day_adjustment cho tất cả các ngày
        today = date.today()

        
        # Tính tổng uplift của các ngày chưa có actual VÀ chưa qua (Weighted Left)
        # Weighted Left = SUM(uplift) của các ngày chưa có actual và chưa qua
        # Loại trừ ngày đã qua vì những ngày đó có weighted_left = 0
        total_weight_left = Decimal('0')
        print(f"\n=== DEBUG: Tính SUM(Weighted Left) (chỉ ngày chưa qua) ===")
        print(f"DEBUG: Today = {today}")
        days_in_weighted_left = []
        for calendar_date, day_data in all_days.items():
            if calendar_date not in days_with_actual and calendar_date > today:
                uplift = day_data['uplift']
                total_weight_left += uplift
                days_in_weighted_left.append((calendar_date, uplift))
                if calendar_date.day <= 20:  # In 20 ngày đầu tiên để debug
                    print(f"DEBUG: Ngày {calendar_date}: date_label={day_data['date_label']}, uplift={uplift}")
        
        print(f"DEBUG: SUM(Weighted Left) = {total_weight_left}")
        print(f"DEBUG: Số ngày có actual: {len(days_with_actual)}")
        print(f"DEBUG: Số ngày chưa có actual và chưa qua: {len(days_in_weighted_left)}")
        print(f"DEBUG: Danh sách ngày trong Weighted Left: {[str(d[0]) for d in days_in_weighted_left]}")
        
        # Tính EOD cho ngày hiện tại (nếu ngày hiện tại nằm trong tháng đang tính)
        eod_value = None
        current_datetime = datetime.now()
        # Làm tròn giờ xuống: nếu 9h05 thì dùng 9h, nếu 9h59 thì vẫn dùng 9h
        # datetime.now().hour tự động làm tròn xuống (9h05 → 9h, 9h59 → 9h)
        current_hour = current_datetime.hour
        current_minute = current_datetime.minute
        
        if today.year == target_year and today.month == target_month and today in all_days:
            print(f"\n=== DEBUG: Tính EOD cho ngày {today} ===")
            print(f"DEBUG: Current datetime = {current_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"DEBUG: Current hour (rounded down) = {current_hour}h")
            print(f"DEBUG: Lấy actual từ 0h00 đến <{current_hour}h (tức là từ 0h00 đến {current_hour - 1}h59)")
            
            # Lấy % doanh thu theo giờ trong 30 ngày gần nhất
            hourly_percentages = self.revenue_helper.get_hourly_revenue_percentage(days_back=30)
            print(f"DEBUG: Hourly revenue percentages (30 ngày gần nhất):")
            for hour in range(24):
                if hour in hourly_percentages:
                    percentage = hourly_percentages[hour]
                    print(f"  - Giờ {hour:2d}h: {percentage:.6f} ({percentage * 100:.4f}%)")
            
            # Tính tổng % của các giờ đã qua (từ 0h đến current_hour - 1)
            # Ví dụ: current_hour = 9 thì tính tổng % từ 0h đến 8h
            total_percentage_passed = Decimal('0')
            print(f"\nDEBUG: Tính tổng % của các giờ đã qua (0h đến {current_hour - 1}h):")
            for hour in range(current_hour):
                if hour in hourly_percentages:
                    percentage = Decimal(str(hourly_percentages[hour]))
                    total_percentage_passed += percentage
                    print(f"  - Giờ {hour:2d}h: {percentage:.6f} ({float(percentage) * 100:.4f}%)")
            
            print(f"DEBUG: Tổng % của các giờ đã qua = {total_percentage_passed} ({float(total_percentage_passed) * 100:.4f}%)")
            
            # Lấy actual từ đầu ngày đến < giờ hiện tại (0h00 đến <current_hour)
            # Ví dụ: current_hour = 9 thì lấy actual từ 0h00 đến 8h59
            actual_until_hour = self.revenue_helper.get_daily_actual_until_hour(today, current_hour)
            print(f"DEBUG: Actual từ 0h00 đến <{current_hour}h (tức là từ 0h00 đến {current_hour - 1}h59) = {actual_until_hour}")
            
            # Tính EOD = actual / sum(% của các giờ đã qua)
            if total_percentage_passed > 0 and actual_until_hour > 0:
                eod_value = float(actual_until_hour / total_percentage_passed)
                print(f"\nDEBUG: EOD = {actual_until_hour} / {total_percentage_passed} = {eod_value}")
            else:
                print(f"\nDEBUG: Không thể tính EOD cho ngày {today}")
                print(f"DEBUG: total_percentage_passed = {total_percentage_passed}")
                print(f"DEBUG: actual_until_hour = {actual_until_hour}")
                if total_percentage_passed == 0:
                    print(f"DEBUG: Lý do: Tổng % của các giờ đã qua = 0 (không có data)")
                if actual_until_hour == 0:
                    print(f"DEBUG: Lý do: Actual từ đầu ngày = 0 (chưa có doanh thu)")
        else:
            print(f"\n=== DEBUG: Không tính EOD ===")
            print(f"DEBUG: Ngày hiện tại ({today}) không nằm trong tháng đang tính ({target_month}/{target_year})")
        
        results = []
        for calendar_date, day_data in all_days.items():
            kpi_day_initial = day_data['kpi_day_initial']
            weight = day_data['weight']
            
            if calendar_date in actuals:
                # Ngày có actual: kpi_day_adjustment = actual
                actual_amount = actuals[calendar_date]
                kpi_day_adjustment = actual_amount
                actual_amount_value = float(actual_amount)
                gap = kpi_day_adjustment - kpi_day_initial
                # weighted_left = 0 cho ngày có actual
                weighted_left = Decimal('0')
            else:
                # Ngày không có actual
                actual_amount_value = None
                uplift = day_data['uplift']
                
                # Nếu ngày đã qua (<= ngày hiện tại) và không có actual
                # Theo query SQL: nếu calendar_date <= today() thì dùng actual (nếu có), nếu không có thì NULL
                # Nhưng trong code, nếu không có actual thì set kpi_day_adjustment = 0 (theo yêu cầu trước đó)
                if calendar_date <= today:
                    kpi_day_adjustment = Decimal('0')
                    gap = Decimal('0') - kpi_day_initial  # gap = -kpi_day_initial
                    weighted_left = Decimal('0')  # weighted_left = 0 cho ngày đã qua và không có actual
                else:
                    # Ngày chưa qua: phân bổ gap
                    # Công thức: kpi_day_adjustment = kpi_day_initial - (SUM(Gap) * Uplift / SUM(Weighted Left))
                    # Trong đó SUM(Gap) = tổng gap của tất cả các ngày có actual
                    # Weighted Left = SUM(uplift) của các ngày chưa có actual và chưa qua
                    # Dùng uplift (không phải weight) trong công thức phân bổ
                    weighted_left = uplift  # weighted_left = uplift cho ngày chưa qua và không có actual
                    if total_weight_left > 0:
                        gap_portion = (total_gap * uplift) / total_weight_left
                        kpi_day_adjustment = kpi_day_initial - gap_portion
                        # Debug log cho tất cả các ngày chưa qua để kiểm tra
                        print(f"DEBUG: Ngày {calendar_date}: kpi_day_initial={kpi_day_initial}, weight={weight}, uplift={uplift}")
                        print(f"  -> gap_portion = ({total_gap} * {uplift}) / {total_weight_left} = {gap_portion}")
                        print(f"  -> kpi_day_adjustment = {kpi_day_initial} - ({gap_portion}) = {kpi_day_adjustment}")
                    else:
                        # Nếu không có ngày nào chưa có actual, giữ nguyên kpi_day_initial
                        kpi_day_adjustment = kpi_day_initial
                    
                    # Ngày chưa qua và không có actual: không có gap (None) - giữ logic cũ
                    gap = None
            
            # Set eod:
            # - Ngày hiện tại: tính EOD theo công thức
            # - Ngày đã qua: eod = actual (luôn có giá trị, nếu không có actual thì = 0)
            # - Ngày chưa qua: eod = None
            if calendar_date == today:
                # Ngày hiện tại: dùng EOD đã tính
                eod = eod_value
            elif calendar_date < today:
                # Ngày đã qua: eod = actual (luôn có giá trị, nếu không có actual thì = 0)
                if calendar_date in actuals:
                    eod = float(actuals[calendar_date])
                else:
                    eod = 0.0  # Nếu không có actual thì eod = 0
            else:
                # Ngày chưa qua: eod = None
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
        """
        Update kpi_day_adjustment vào bảng kpi_day cho TẤT CẢ các ngày trong tháng.
        Lấy kpi_month mới nhất từ bảng kpi_month để đảm bảo nhất quán.
        """
        if not kpi_day_adjustment_data:
            return
        
        now = datetime.now()
        
        # Lấy danh sách các tháng cần lấy kpi_month
        months_needed = set()
        for row in kpi_day_adjustment_data:
            months_needed.add((row['year'], row['month']))
        
        # Lấy kpi_month.kpi_initial với version tương ứng với tháng đó
        # Tháng 1 → version "Thang 1", Tháng 2 → version "Thang 2", ...
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
        
        # Lấy dữ liệu hiện tại từ kpi_day (chỉ lấy các field cần thiết, không lấy kpi_month)
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
        
        # Lấy actual từ transactions cho tất cả các ngày
        actual_map = self.revenue_helper.get_daily_actual_by_dates(calendar_dates)
        
        # Tạo data để insert (ReplacingMergeTree sẽ tự động merge)
        data = []
        for row in kpi_day_adjustment_data:
            calendar_date = row['calendar_date']
            current_data = current_data_map.get(calendar_date, {})
            
            # Lấy kpi_month mới nhất từ kpi_month_map (từ bảng kpi_month)
            year = row['year']
            month = row['month']
            kpi_month = kpi_month_map.get((year, month), 0)
            
            # Lấy actual và tính gap, weighted_left
            kpi_day_initial = current_data.get('kpi_day_initial', row['kpi_day_initial'])
            uplift = current_data.get('uplift', row.get('uplift', 0))
            today = date.today()
            
            # Chỉ set actual = 0 cho ngày hiện tại và ngày đã qua
            # Ngày chưa tới thì actual = None
            if calendar_date <= today:
                actual = actual_map.get(calendar_date, 0)  # Nếu không có actual thì = 0
            else:
                actual = actual_map.get(calendar_date)  # Ngày chưa tới: None nếu không có
            
            # Tính weighted_left và gap
            # actual đã được set = 0 nếu không có (cho ngày <= today), nên chỉ cần check != 0
            if actual is not None and actual != 0:
                # Ngày có actual (actual > 0): gap = actual - kpi_day_initial, weighted_left = 0
                weighted_left = 0
                gap = actual - kpi_day_initial
            elif calendar_date < today:
                # Ngày đã qua và không có actual (actual = 0): gap = -kpi_day_initial, weighted_left = 0
                weighted_left = 0
                gap = 0 - kpi_day_initial
            else:
                # Ngày chưa qua và không có actual: weighted_left = uplift, gap = None (giữ logic cũ)
                weighted_left = uplift
                gap = None
            
            # Xử lý eod: nếu None thì giữ None (giống như actual và gap)
            # ClickHouse sẽ tự động xử lý None cho Nullable columns
            eod_value = row.get('eod')
            
            data.append([
                calendar_date,
                row['year'],
                row['month'],
                row['day'],
                row['date_label'],
                kpi_month,  # Lấy từ kpi_month mới nhất, không lấy từ current_data
                current_data.get('uplift', row.get('uplift', 0)),
                current_data.get('weight', 0),
                current_data.get('total_weight_month', 0),
                current_data.get('kpi_day_initial', row['kpi_day_initial']),
                actual,  # actual từ transactions (có thể None)
                gap,     # gap được tính theo logic mới (có thể None)
                row['kpi_day_adjustment'],
                weighted_left,  # weighted_left: 0 nếu có actual, = uplift nếu không có actual
                eod_value,  # eod: chỉ có giá trị cho ngày hiện tại, None cho các ngày khác
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
        """
        Tính và lưu kpi_day_adjustment cho TẤT CẢ các ngày trong tháng:
        - Ngày có actual: kpi_day_adjustment = actual
        - Ngày không có actual: kpi_day_adjustment = kpi_day_initial + (Gap * uplift / SUM(uplift của các ngày chưa có actual))
        """
        kpi_day_adjustment_data = self.calculate_kpi_day_adjustment(
            target_year=target_year,
            target_month=target_month
        )
        
        self.update_kpi_day_adjustment(kpi_day_adjustment_data)
        
        return kpi_day_adjustment_data


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIDayCalculator(constants)
    
    target_year = 2026
    target_month = 1
    
    # Tính kpi_day_initial: luôn tính lại để đảm bảo dùng kpi_month.kpi_initial mới nhất
    # Query đã dùng FINAL nên sẽ tự động lấy version mới nhất của kpi_month
    print(f"Calculating kpi_day_initial for month {target_month}/{target_year}...")
    kpi_day_initial_data = calculator.calculate_and_save_kpi_day_initial(
        target_year=target_year,
        target_month=target_month
    )
    print(f"Successfully saved {len(kpi_day_initial_data)} kpi_day_initial records")
    
    # Kiểm tra xem có actual không, nếu có thì tính kpi_day_adjustment
    check_actual_query = f"""
        SELECT COUNT(*) as cnt
        FROM hskcdp.actual_2026_day_staging FINAL
        WHERE year = {target_year}
          AND month = {target_month}
          AND processed = true
    """
    actual_check_result = calculator.client.query(check_actual_query)
    has_actual = actual_check_result.result_rows[0][0] > 0 if actual_check_result.result_rows else False
    
    if has_actual:
        print(f"Calculating kpi_day_adjustment for month {target_month}/{target_year}...")
        kpi_day_adjustment_data = calculator.calculate_and_save_kpi_day_adjustment(
            target_year=target_year,
            target_month=target_month
        )
        print(f"Successfully saved {len(kpi_day_adjustment_data)} kpi_day_adjustment records")
    else:
        print(f"No processed actuals found for month {target_month}/{target_year}. Skipping kpi_day_adjustment calculation.")