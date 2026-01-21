from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIAdjustmentCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def get_avg_rev_normal_day_30_days(self) -> Decimal:
        return self.revenue_helper.get_avg_rev_normal_day_30_days()
    
    def calculate_eom(self, target_year: int, target_month: int) -> Optional[Decimal]:
        sum_actual = self.revenue_helper.get_daily_actual_sum(target_year, target_month)
        
        if sum_actual == 0:
            return None
        
        actual_dates = self.revenue_helper.get_actual_dates(target_year, target_month)
        print(f"DEBUG: actual_dates = {actual_dates}")
        
        if not actual_dates:
            return None
        
        today = date.today()
        
        if target_month == 12:
            last_day_of_month = date(target_year, 12, 31)
        else:
            next_month = date(target_year, target_month + 1, 1)
            last_day_of_month = next_month - timedelta(days=1)
        
        start_date = None
        if today > last_day_of_month:
            remaining_days_by_label = {}
            print(f"DEBUG: today = {today}, tháng đã kết thúc, remaining_days_by_label = {remaining_days_by_label}")
        else:
            if today.year == target_year and today.month == target_month:
                start_date = today + timedelta(days=1)
            else:
                start_date = date(target_year, target_month, 1)
            
            remaining_days_count_query = f"""
                SELECT 
                    date_label,
                    COUNT(*) as so_ngay
                FROM dim_date
                WHERE year = {target_year}
                  AND month = {target_month}
                  AND calendar_date >= '{start_date}'
                  AND calendar_date <= '{last_day_of_month}'
                  AND NOT (
                      (month = 6 AND day = 6) OR
                      (month = 9 AND day = 9) OR
                      (month = 11 AND day = 11) OR
                      (month = 12 AND day = 12)
                  )
                GROUP BY date_label
            """
            
            remaining_days_result = self.client.query(remaining_days_count_query)
            remaining_days_by_label = {row[0]: int(row[1]) for row in remaining_days_result.result_rows}
            print(f"DEBUG: today = {today}, start_date = {start_date}, remaining_days_by_label = {remaining_days_by_label}")
        
        avg_total_normal_day = self.get_avg_rev_normal_day_30_days()
        metadata_query = f"""
            SELECT 
                date_label,
                uplift,
                row_number() OVER (
                    PARTITION BY year, month, date_label
                    ORDER BY updated_at DESC
                ) AS rn
            FROM hskcdp.kpi_day_metadata
            WHERE year = {target_year}
              AND month = {target_month}
        """
        
        metadata_result = self.client.query(metadata_query)
        metadata_by_label = {}
        
        for row in metadata_result.result_rows:
            if row[2] == 1:
                date_label = row[0]
                uplift = Decimal(str(row[1]))
                metadata_by_label[date_label] = uplift
        
        sum_rev_eom = Decimal('0')
        for date_label, so_ngay in remaining_days_by_label.items():
            uplift = metadata_by_label.get(date_label, Decimal('1.0'))
            rev_eom_for_label = Decimal(str(so_ngay)) * avg_total_normal_day * uplift
            sum_rev_eom += rev_eom_for_label
        
        print(f"DEBUG EOM calculation for month {target_month}:")
        print(f"  - Sum(actual) = {sum_actual}")
        print(f"  - Today = {today}")
        if today <= last_day_of_month:
            print(f"  - Start date (remaining from) = {start_date}")
        print(f"  - Avg total normal day (30 ngày gần nhất) = {avg_total_normal_day}")
        print(f"  - Remaining days by label: {remaining_days_by_label}")
        print(f"  - Sum(rev eom) = {sum_rev_eom}")
        print(f"  - EOM = {sum_actual + sum_rev_eom}")
        
        eom = sum_actual + sum_rev_eom
        
        return eom
    
    def is_month_ended(self, target_year: int, target_month: int) -> bool:
        return self.revenue_helper.check_month_ended(target_year, target_month)
    
    def get_current_version_number(self, target_year: int, target_month: int) -> int:
        """
        Xác định version number hiện tại dựa trên ngày.
        Logic:
        - Ngày <= 25: version = tháng hiện tại
        - Ngày >= 26 trong tháng hiện tại: version = tháng hiện tại (vẫn dùng version cũ)
        - Ngày 1 tháng sau: version = tháng tiếp theo (chuyển sang version mới)
        """
        today = date.today()
        
        # Nếu đang ở tháng khác với target_month → đã qua tháng mới
        if today.year != target_year or today.month != target_month:
            # Đã qua tháng mới → dùng version của tháng tiếp theo
            if target_month == 12:
                return 1  # Năm sau
            else:
                return target_month + 1
        else:
            # Vẫn trong tháng hiện tại → luôn dùng version của tháng hiện tại
            return target_month
    
    def get_baseline_version_number(self, current_version_number: int) -> int:
        """
        Lấy baseline version number (version trước đó).
        Logic:
        - Version 1: baseline = 1 (seed sẵn)
        - Version khác: baseline = version trước đó
        """
        if current_version_number == 1:
            return 1
        else:
            return current_version_number - 1
    
    def create_new_version_from_day_26(self, target_year: int, target_month: int) -> None:
        """
        Chốt số vào ngày 26: Lấy kpi_adjustment của version hiện tại → làm kpi_initial cho version tiếp theo.
        Chỉ chạy khi today.day == 26 và today.month == target_month.
        """
        today = date.today()

        # Chỉ chạy vào ngày 26 của tháng hiện tại
        if today.day != 26 or today.month != target_month or today.year != target_year:
            return
        
        current_version = f"Thang {target_month}"
        
        # Xác định version tiếp theo
        if target_month == 12:
            next_version_number = 1
            next_year = target_year + 1
        else:
            next_version_number = target_month + 1
            next_year = target_year
        
        next_version = f"Thang {next_version_number}"
        
        print(f"DEBUG: Chốt số vào ngày 26 - Tạo version mới")
        print(f"  - Current version: {current_version}")
        print(f"  - Next version: {next_version} (năm {next_year})")
        
        # Kiểm tra version mới đã tồn tại chưa
        check_query = f"""
            SELECT COUNT(*) as cnt
            FROM hskcdp.kpi_month FINAL
            WHERE year = {next_year}
              AND version = '{next_version}'
        """
        check_result = self.client.query(check_query)
        if check_result.result_rows and check_result.result_rows[0][0] > 0:
            print(f"  - Version '{next_version}' đã tồn tại, bỏ qua việc tạo mới")
            return
        
        # Lấy kpi_adjustment mới nhất của version hiện tại cho 12 tháng
        current_version_query = f"""
            SELECT
                month,
                kpi_adjustment
            FROM (
                SELECT
                    month,
                    kpi_adjustment,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month FINAL
                WHERE year = {target_year}
                  AND version = '{current_version}'
            )
            WHERE rn = 1
            ORDER BY month
        """
        
        current_version_result = self.client.query(current_version_query)
        current_kpi_adjustments = {int(row[0]): float(row[1]) for row in current_version_result.result_rows}
        
        if len(current_kpi_adjustments) != 12:
            print(f"  - WARNING: Version '{current_version}' chưa có đủ 12 tháng, không thể tạo version mới")
            return
        
        print(f"  - Lấy kpi_adjustment từ version '{current_version}' cho 12 tháng:")
        for month in sorted(current_kpi_adjustments.keys()):
            print(f"    Tháng {month}: {current_kpi_adjustments[month]}")
        
        # Tạo version mới với kpi_initial = kpi_adjustment của version cũ
        now = datetime.now()
        data = []
        
        for month in range(1, 13):
            kpi_initial = current_kpi_adjustments[month]
            
            data.append([
                next_version,
                next_year,
                month,
                kpi_initial,  # kpi_initial = kpi_adjustment của version cũ
                None,  # actual_2026
                None,  # gap
                None,  # eom
                kpi_initial,  # kpi_adjustment (ban đầu = kpi_initial)
                now,  # created_at
                now   # updated_at
            ])
        
        columns = [
            'version', 'year', 'month', 'kpi_initial', 'actual_2026', 'gap',
            'eom', 'kpi_adjustment', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_month", data, column_names=columns)
        print(f"  - Đã tạo version '{next_version}' với kpi_initial từ version '{current_version}'")
    
    def calculate_kpi_adjustment(self, target_month: Optional[int] = None) -> List[Dict]:
        if target_month is None:
            target_month = self.revenue_helper.get_max_month_with_actual(self.constants.KPI_YEAR_2026)
            
            if target_month == 0:
                target_month = 1
        
        version = f"Thang {target_month}"

        baseline_version = "Thang 1"
        baseline_query = f"""
            SELECT
                month,
                kpi_initial
            FROM hskcdp.kpi_month FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
              AND version = '{baseline_version}'
            ORDER BY month
        """
        baseline_result = self.client.query(baseline_query)
        baseline_kpi = {int(row[0]): float(row[1]) for row in baseline_result.result_rows}

        missing_months = [m for m in range(1, 13) if m not in baseline_kpi]
        if missing_months:
            raise ValueError(
                f"Thiếu kpi_initial cho version '{baseline_version}' ở các tháng: {missing_months}. "
                f"Vui lòng seed đủ 12 tháng trước khi chạy pipeline."
            )

        base_kpi = {}
        for month in range(1, 13):
            kpi_initial = baseline_kpi[month]
            
            base_kpi[month] = {'year': self.constants.KPI_YEAR_2026, 'month': month, 'kpi_initial': kpi_initial}
        
        actuals_month = self.revenue_helper.get_monthly_actual(self.constants.KPI_YEAR_2026)
        
        eoms = {}
        actuals_day = {}
        gaps = {}
        total_gap = Decimal('0')
        
        for month in range(1, 13):
            kpi_initial = Decimal(str(base_kpi[month]['kpi_initial']))
            
            eom = self.calculate_eom(self.constants.KPI_YEAR_2026, month)
            
            if eom is not None:
                eoms[month] = eom
                gap = eom - kpi_initial
                gaps[month] = gap
                
                actuals_day[month] = self.revenue_helper.get_daily_actual_sum(
                    self.constants.KPI_YEAR_2026, month
                )
                
                total_gap += gap
            elif month in actuals_month:
                actual = Decimal(str(actuals_month[month]))
                gap = actual - kpi_initial
                gaps[month] = gap
                total_gap += gap
        
        months_with_actual = set(eoms.keys()) | set(actuals_month.keys())
        remaining_months = 12 - len(months_with_actual)
        
        if remaining_months > 0:
            gap_per_remaining_month = total_gap / Decimal(str(remaining_months))
        else:
            gap_per_remaining_month = Decimal('0')
        
        print(f"DEBUG Gap distribution:")
        print(f"  - Total gap = {total_gap}")
        print(f"  - Months with actual (EOM or monthly): {sorted(months_with_actual)}")
        print(f"  - Remaining months = {remaining_months}")
        print(f"  - Gap per remaining month = {gap_per_remaining_month}")
        
        
        results = []
        for month in range(1, 13):
            kpi_initial = Decimal(str(base_kpi[month]['kpi_initial']))
            
            if month in eoms:
                eom_value = eoms[month]
                gap = gaps[month]
                kpi_adjustment = eom_value
                actual_2026 = actuals_day[month]
            elif month in actuals_month:
                actual_2026 = Decimal(str(actuals_month[month]))
                gap = gaps[month]
                kpi_adjustment = actual_2026
            else:
                actual_2026 = None
                gap = None
                kpi_adjustment = kpi_initial - gap_per_remaining_month
            
            results.append({
                'version': version,
                'year': self.constants.KPI_YEAR_2026,
                'month': month,
                'kpi_initial': float(kpi_initial),
                'actual_2026': float(actual_2026) if actual_2026 is not None else None,
                'gap': float(gap) if gap is not None else None,
                'eom': float(eoms[month]) if month in eoms else None,
                'kpi_adjustment': float(kpi_adjustment)
            })
        
        return results
    
    def save_kpi_adjustment(self, target_month: Optional[int] = None) -> List[Dict]:
        if target_month is None:
            target_month = self.revenue_helper.get_max_month_with_actual(self.constants.KPI_YEAR_2026)
            
            if target_month == 0:
                target_month = 1
        
        # QUAN TRỌNG: Tính toán và update version hiện tại TRƯỚC
        # Sau đó mới chốt số và tạo version mới (để đảm bảo lấy kpi_adjustment mới nhất)
        results = self.calculate_kpi_adjustment(target_month)
        
        now = datetime.now()
        
        version = results[0]['version'] if results else None
        existing_created_at_query = f"""
            SELECT month, created_at
            FROM hskcdp.kpi_month FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
              AND version = '{version}'
        """
        existing_created_at_result = self.client.query(existing_created_at_query)
        existing_created_at = {row[0]: row[1] for row in existing_created_at_result.result_rows}
        
        data = []
        for row in results:
            month = row['month']
            created_at = existing_created_at.get(month, now)
            
            data.append([
                row['version'],
                row['year'],
                row['month'],
                row['kpi_initial'],
                row['actual_2026'],
                row['gap'],
                row['eom'],
                row['kpi_adjustment'],
                created_at,
                now
            ])
        
        columns = [
            'version', 'year', 'month', 'kpi_initial', 'actual_2026', 'gap',
            'eom', 'kpi_adjustment', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_month", data, column_names=columns)
        
        # SAU KHI đã update version hiện tại → mới chốt số và tạo version mới
        # (để đảm bảo lấy kpi_adjustment mới nhất)
        # Kiểm tra: nếu là ngày 26 của tháng hiện tại → chốt số và tạo version mới
        today = date.today()
        if today.day == 26 and today.month == target_month and today.year == self.constants.KPI_YEAR_2026:
            print(f"\n=== CHỐT SỐ VÀO NGÀY 26 ===")
            self.create_new_version_from_day_26(self.constants.KPI_YEAR_2026, target_month)
            print(f"=== HOÀN TẤT CHỐT SỐ ===\n")
        
        return results


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIAdjustmentCalculator(constants)
    
    print("Calculating KPI adjustment...")
    result = calculator.save_kpi_adjustment()
    
    print(f"Successfully saved {len(result)} records to kpi_month")
