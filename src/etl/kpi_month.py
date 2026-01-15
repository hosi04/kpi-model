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
        """
        Lấy avg revenue của Normal day từ 30 ngày gần nhất từ transactions
        Sử dụng helper method từ RevenueQueryHelper
        """
        return self.revenue_helper.get_avg_rev_normal_day_30_days()
    
    def calculate_eom(self, target_year: int, target_month: int) -> Optional[Decimal]:
        """
        Tính EOM cho một tháng:
        EOM = Sum(actual) + Sum(rev eom những ngày còn lại)
        Sum(rev eom) = Avg rev normal day * uplift cho mỗi ngày còn lại
        """
        # 1. Tính Sum(actual) từ transactions
        sum_actual = self.revenue_helper.get_daily_actual_sum(target_year, target_month)
        
        # Nếu không có actual nào, không tính EOM
        if sum_actual == 0:
            return None
        
        # 2. Tính số ngày còn lại từ ngày hiện tại đến cuối tháng
        # Lấy danh sách các ngày đã có actual từ transactions (để debug)
        actual_dates = self.revenue_helper.get_actual_dates(target_year, target_month)
        print(f"DEBUG: actual_dates = {actual_dates}")
        
        if not actual_dates:
            # Không có actual nào, không tính EOM
            return None
        
        # Lấy ngày hiện tại
        today = date.today()
        
        # Tính ngày cuối cùng của tháng
        if target_month == 12:
            last_day_of_month = date(target_year, 12, 31)
        else:
            next_month = date(target_year, target_month + 1, 1)
            last_day_of_month = next_month - timedelta(days=1)
        
        # Nếu ngày hiện tại đã qua tháng này, không có ngày còn lại
        start_date = None
        if today > last_day_of_month:
            # Tháng đã kết thúc, không có ngày còn lại
            remaining_days_by_label = {}
            print(f"DEBUG: today = {today}, tháng đã kết thúc, remaining_days_by_label = {remaining_days_by_label}")
        else:
            # Đếm số ngày còn lại từ ngày hiện tại đến cuối tháng theo date_label
            # Nếu today trong tháng này, tính từ today + 1 (ngày mai) đến cuối tháng
            # Nếu today chưa đến tháng này, tính từ đầu tháng đến cuối tháng
            if today.year == target_year and today.month == target_month:
                start_date = today + timedelta(days=1)  # Từ ngày mai
            else:
                # Nếu today chưa đến tháng này, tính từ đầu tháng
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
        
        # 3. Lấy uplift của các date_label từ kpi_day_metadata (baseline Normal day lấy từ 30 ngày gần nhất)
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
            if row[2] == 1:  # Chỉ lấy record mới nhất (rn = 1)
                date_label = row[0]
                uplift = Decimal(str(row[1]))
                metadata_by_label[date_label] = uplift
        
        # 4. Tính Sum(rev eom) = SUM(số_ngày * avg_rev_normal_day * uplift) cho từng date_label
        sum_rev_eom = Decimal('0')
        for date_label, so_ngay in remaining_days_by_label.items():
            uplift = metadata_by_label.get(date_label, Decimal('1.0'))
            rev_eom_for_label = Decimal(str(so_ngay)) * avg_total_normal_day * uplift
            sum_rev_eom += rev_eom_for_label
        
        # Debug log
        print(f"DEBUG EOM calculation for month {target_month}:")
        print(f"  - Sum(actual) = {sum_actual}")
        print(f"  - Today = {today}")
        if today <= last_day_of_month:
            print(f"  - Start date (remaining from) = {start_date}")
        print(f"  - Avg total normal day (30 ngày gần nhất) = {avg_total_normal_day}")
        print(f"  - Remaining days by label: {remaining_days_by_label}")
        print(f"  - Sum(rev eom) = {sum_rev_eom}")
        print(f"  - EOM = {sum_actual + sum_rev_eom}")
        
        # 5. EOM = Sum(actual) + Sum(rev eom)
        eom = sum_actual + sum_rev_eom
        
        return eom
    
    def is_month_ended(self, target_year: int, target_month: int) -> bool:
        """
        Check xem tháng đã kết thúc chưa
        Sử dụng helper method từ RevenueQueryHelper
        """
        return self.revenue_helper.check_month_ended(target_year, target_month)
    
    def calculate_kpi_adjustment(self, target_month: Optional[int] = None) -> List[Dict]:
        """
        Tính kpi_adjustment cho một tháng cụ thể hoặc tháng có actual mới nhất
        
        Args:
            target_month: Tháng cần tính (1-12). Nếu None, tự động xác định tháng có actual mới nhất
        """
        # Xác định tháng đang tính (target_month)
        if target_month is None:
            # Tìm tháng có actual mới nhất từ transactions
            target_month = self.revenue_helper.get_max_month_with_actual(self.constants.KPI_YEAR_2026)
            
            if target_month == 0:
                # Chưa có actual nào, tính cho tháng 1
                target_month = 1
        
        # Version = tháng đang tính (1-12, không có 0)
        version = f"Thang {target_month}"
        
        # Lấy kpi_initial: ưu tiên từ kpi_month_base, nếu chưa có thì từ kpi_month (version hiện tại)
        base_query = f"""
            SELECT 
                year,
                month,
                kpi_initial
            FROM hskcdp.kpi_month_base
            WHERE year = {self.constants.KPI_YEAR_2026}
            ORDER BY month
        """
        base_result = self.client.query(base_query)
        base_kpi_from_base = {row[1]: float(row[2]) for row in base_result.result_rows}
        
        # Lấy kpi_initial từ kpi_month (nếu đã có record với version này)
        existing_query = f"""
            SELECT 
                month,
                kpi_initial
            FROM hskcdp.kpi_month FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
              AND version = '{version}'
            ORDER BY month
        """
        existing_result = self.client.query(existing_query)
        existing_kpi = {row[0]: float(row[1]) for row in existing_result.result_rows}
        
        # Kết hợp: ưu tiên từ existing, nếu không có thì từ base
        base_kpi = {}
        for month in range(1, 13):
            if month in existing_kpi:
                kpi_initial = existing_kpi[month]
            elif month in base_kpi_from_base:
                kpi_initial = base_kpi_from_base[month]
            else:
                # Nếu không có trong cả 2, lấy từ kpi_month (version trước đó)
                fallback_query = f"""
                    SELECT kpi_initial
                    FROM hskcdp.kpi_month FINAL
                    WHERE year = {self.constants.KPI_YEAR_2026}
                      AND month = {month}
                    ORDER BY updated_at DESC
                    LIMIT 1
                """
                fallback_result = self.client.query(fallback_query)
                if fallback_result.result_rows:
                    kpi_initial = float(fallback_result.result_rows[0][0])
                else:
                    kpi_initial = 0
            
            base_kpi[month] = {'year': self.constants.KPI_YEAR_2026, 'month': month, 'kpi_initial': kpi_initial}
        
        # Lấy actual tháng (nếu có) từ transactions
        actuals_month = self.revenue_helper.get_monthly_actual(self.constants.KPI_YEAR_2026)
        
        # Tính EOM cho các tháng có actual theo ngày
        eoms = {}
        actuals_day = {}  # Lưu tổng actual theo ngày cho các tháng có EOM
        gaps = {}
        total_gap = Decimal('0')
        months_with_ended = []  # Các tháng đã kết thúc (có actual ngày cuối)
        
        for month in range(1, 13):
            kpi_initial = Decimal(str(base_kpi[month]['kpi_initial']))
            
            # Check xem có actual theo ngày không
            eom = self.calculate_eom(self.constants.KPI_YEAR_2026, month)
            
            if eom is not None:
                # Có actual theo ngày → tính EOM, gap = EOM - kpi_initial
                eoms[month] = eom
                gap = eom - kpi_initial
                gaps[month] = gap
                
                # Lấy tổng actual theo ngày để lưu vào actual_2026 từ transactions
                actuals_day[month] = self.revenue_helper.get_daily_actual_sum(
                    self.constants.KPI_YEAR_2026, month
                )
                
                # Check xem tháng đã kết thúc chưa (ngày cuối cùng có actual)
                if self.is_month_ended(self.constants.KPI_YEAR_2026, month):
                    months_with_ended.append(month)
                    total_gap += gap
            elif month in actuals_month:
                # Không có actual theo ngày nhưng có actual tháng → giữ logic cũ
                actual = Decimal(str(actuals_month[month]))
                gap = actual - kpi_initial
                gaps[month] = gap
                total_gap += gap
        
        # Tính gap phân bổ (chỉ từ các tháng đã kết thúc)
        months_with_actual = len(actuals_month)
        remaining_months = 12 - months_with_actual
        
        if remaining_months > 0:
            gap_per_remaining_month = total_gap / Decimal(str(remaining_months))
        else:
            gap_per_remaining_month = Decimal('0')
        
        # Version đã được xác định ở đầu function
        
        results = []
        for month in range(1, 13):
            kpi_initial = Decimal(str(base_kpi[month]['kpi_initial']))
            
            if month in eoms:
                # Có actual theo ngày → dùng EOM
                eom_value = eoms[month]
                gap = gaps[month]
                kpi_adjustment = eom_value
                # actual_2026 = tổng actual của các ngày (để khi cuối tháng sẽ bằng actual của cả tháng)
                actual_2026 = actuals_day[month]
            elif month in actuals_month:
                # Không có actual theo ngày nhưng có actual tháng → giữ logic cũ
                actual_2026 = Decimal(str(actuals_month[month]))
                gap = gaps[month]
                kpi_adjustment = actual_2026
            else:
                # Tháng chưa có actual → phân bổ gap
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
        results = self.calculate_kpi_adjustment(target_month)
        
        now = datetime.now()
        
        # Lấy created_at từ record hiện có (nếu có) để giữ nguyên
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
            # Giữ nguyên created_at nếu đã có record, nếu không thì dùng now
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
                created_at,  # Giữ nguyên created_at nếu đã có
                now         # updated_at luôn là now
            ])
        
        columns = [
            'version', 'year', 'month', 'kpi_initial', 'actual_2026', 'gap',
            'eom', 'kpi_adjustment', 'created_at', 'updated_at'
        ]
        
        # ReplacingMergeTree sẽ tự động merge dựa trên ORDER BY key (version, year, month)
        self.client.insert("hskcdp.kpi_month", data, column_names=columns)
        
        return results


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIAdjustmentCalculator(constants)
    
    print("Calculating KPI adjustment...")
    result = calculator.save_kpi_adjustment()
    
    print(f"Successfully saved {len(result)} records to kpi_month")
