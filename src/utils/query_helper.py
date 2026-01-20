from decimal import Decimal
from datetime import date, timedelta, datetime
from typing import Dict, Set, List
from src.utils.clickhouse_client import get_client


class RevenueQueryHelper:
    def __init__(self):
        self.client = get_client()

    # KPI MONTH RELATED QUERIES
    
    def get_avg_rev_normal_day_30_days(self) -> Decimal:
        """
        Lấy avg revenue của Normal day từ 30 ngày gần nhất từ transactions
        """
        query = f"""
            SELECT
                AVG(daily_revenue) AS avg_rev_normal_day
            FROM (
                SELECT
                    toDate(t.created_at) as calendar_date,
                    SUM(t.transaction_total) AS daily_revenue
                FROM hskcdp.object_sql_transactions AS t FINAL
                INNER JOIN hskcdp.dim_date d
                    ON toDate(t.created_at) = d.calendar_date
                WHERE d.date_label = 'Normal day'
                    AND toDate(t.created_at) >= today() - 30
                    AND t.status NOT IN ('Canceled', 'Cancel')
                    AND (toMonth(t.created_at), toDayOfMonth(t.created_at)) NOT IN (
                        (6,6), (9,9), (11,11), (12,12)
                    )
                GROUP BY calendar_date
            )
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return Decimal(str(result.result_rows[0][0]))
        else:
            raise ValueError("Cannot calculate avg rev normal day: no data found")
    
    def get_daily_actual_sum(self, target_year: int, target_month: int) -> Decimal:
        """
        Tính tổng actual revenue theo ngày từ transactions
        """
        query = f"""
            SELECT 
                SUM(t.transaction_total) as sum_actual
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toYear(t.created_at) = {target_year}
              AND toMonth(t.created_at) = {target_month}
              AND t.status NOT IN ('Canceled', 'Cancel')
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return Decimal(str(result.result_rows[0][0]))
        else:
            return Decimal('0')
    
    def get_actual_dates(self, target_year: int, target_month: int) -> Set[date]:
        """
        Lấy danh sách các ngày có actual từ transactions
        Returns: set of date objects
        """
        query = f"""
            SELECT 
                DISTINCT toDate(t.created_at) as calendar_date
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toYear(t.created_at) = {target_year}
                AND toMonth(t.created_at) = {target_month}
                AND t.status NOT IN ('Canceled', 'Cancel')
        """
        
        result = self.client.query(query)
        actual_dates = {row[0] for row in result.result_rows}
        return actual_dates
    
    def get_actual_days_by_label(
        self, 
        target_year: int, 
        target_month: int, 
        actual_dates: Set[date]
    ) -> Dict[str, int]:
        """
        Lấy số lượng ngày đã có actual theo date_label từ transactions
        Returns: dict {date_label: count}
        """
        if not actual_dates:
            return {}
        
        dates_str = ','.join([f"'{d}'" for d in actual_dates])
        
        query = f"""
            SELECT 
                d.date_label,
                COUNT(DISTINCT toDate(t.created_at)) as so_ngay
            FROM hskcdp.object_sql_transactions AS t FINAL
            INNER JOIN hskcdp.dim_date d
                ON toDate(t.created_at) = d.calendar_date
            WHERE toYear(t.created_at) = {target_year}
              AND toMonth(t.created_at) = {target_month}
              AND toDate(t.created_at) IN ({dates_str})
              AND t.status NOT IN ('Canceled', 'Cancel')
              AND (toMonth(t.created_at), toDayOfMonth(t.created_at)) NOT IN (
                    (6,6), (9,9), (11,11), (12,12)
            )
            GROUP BY d.date_label
        """
        
        result = self.client.query(query)
        actual_days_by_label = {row[0]: int(row[1]) for row in result.result_rows}
        return actual_days_by_label
    
    def check_month_ended(self, target_year: int, target_month: int) -> bool:
        """
        Check xem tháng đã kết thúc chưa
        Logic: Nếu ngày hiện tại đã qua ngày cuối cùng của tháng thì tháng đã kết thúc
        """
        # Tính ngày cuối cùng của tháng
        if target_month == 12:
            last_day = date(target_year, 12, 31)
        else:
            next_month = date(target_year, target_month + 1, 1)
            last_day = next_month - timedelta(days=1)
        
        # Check xem ngày hiện tại đã qua ngày cuối cùng chưa
        today = date.today()
        if today > last_day:
            return True
        return False
    
    def get_max_month_with_actual(self, target_year: int) -> int:
        """
        Tìm tháng có actual mới nhất từ transactions
        """
        query = f"""
            SELECT MAX(toMonth(t.created_at)) as max_month
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toYear(t.created_at) = {target_year}
              AND t.status NOT IN ('Canceled', 'Cancel')
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return int(result.result_rows[0][0])
        else:
            return 0
    
    def get_monthly_actual(self, target_year: int) -> Dict[int, float]:
        """
        Lấy actual revenue theo tháng từ transactions
        Returns: dict {month: actual_amount}
        """
        query = f"""
            SELECT 
                toMonth(t.created_at) as month,
                SUM(t.transaction_total) as actual_amount
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toYear(t.created_at) = {target_year}
              AND t.status NOT IN ('Canceled', 'Cancel')
            GROUP BY month
            ORDER BY month
        """
        
        result = self.client.query(query)
        actuals_month = {row[0]: float(row[1]) for row in result.result_rows}
        return actuals_month
    
    # KPI DAY METADATA RELATED QUERIES (HISTORICAL REVENUE QUERIES)
    
    def get_historical_revenue_by_date_label(
        self,
        historical_start_date: date,
        historical_end_date: date,
        date_labels: List[str],
        historical_months: List[int],
        historical_year: int
    ) -> Dict[str, Dict]:
        """
        Lấy historical revenue data theo date_label từ transactions
        Thay thế query từ revenue_2025_ver2
        Returns: dict {date_label: {'avg_total': float, 'so_ngay_historical': int}}
        """
        date_labels_str = ','.join([f"'{dl}'" for dl in date_labels])
        
        # Tính các tháng có thể cross-year
        month_filters = []
        first_month = historical_months[0]
        
        for month in historical_months:
            # Xác định năm cho từng tháng
            # Nếu month < first_month thì đã cross-year (ví dụ: [11, 12, 1] -> tháng 1 ở năm sau)
            if month < first_month:
                month_year = historical_year + 1
            else:
                month_year = historical_year
            month_filters.append(f"toDate('{month_year}-{month:02d}-01')")
        
        query = f"""
            SELECT 
                a.date_label,
                AVG(a.daily_revenue) as avg_total,
                COUNT(DISTINCT a.calendar_date) as so_ngay_historical
            FROM (
                SELECT 
                    d.calendar_date,
                    d.date_label,
                    SUM(t.transaction_total) as daily_revenue
                FROM hskcdp.object_sql_transactions AS t FINAL
                INNER JOIN hskcdp.dim_date d
                    ON toDate(t.created_at) = d.calendar_date
                WHERE toDate(t.created_at) >= today() - INTERVAL 3 MONTH
                  AND d.date_label IN ({date_labels_str})
                  AND t.status NOT IN ('Cancel', 'Canceled')
                  AND (toMonth(t.created_at), toDayOfMonth(t.created_at)) NOT IN (
                        (6,6), (9,9), (11,11), (12,12)
                    )
                GROUP BY 
                    d.calendar_date,
                    d.date_label
            ) as a
            GROUP BY a.date_label
        """
        
        result = self.client.query(query)
        historical_data = {}
        
        for row in result.result_rows:
            date_label = row[0]
            avg_total = float(row[1])
            so_ngay_historical = int(row[2])
            historical_data[date_label] = {
                'avg_total': avg_total,
                'so_ngay_historical': so_ngay_historical
            }
        
        return historical_data


    # KPI DAY RELATED QUERIES
    
    def get_daily_actual_by_dates(self, calendar_dates: List[date]) -> Dict[date, float]:
        """
        Lấy actual revenue theo danh sách ngày cụ thể từ transactions
        Thay thế query từ actual_2026_day_staging
        Returns: dict {calendar_date: actual_amount}
        """
        if not calendar_dates:
            return {}
        
        dates_str = ','.join([f"'{d}'" for d in calendar_dates])
        
        query = f"""
            SELECT 
                toDate(t.created_at) as calendar_date,
                SUM(t.transaction_total) as actual_amount
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toDate(t.created_at) IN ({dates_str})
              AND t.status NOT IN ('Canceled', 'Cancel')
            GROUP BY calendar_date
        """
        
        result = self.client.query(query)
        actual_map = {row[0]: float(row[1]) for row in result.result_rows}
        return actual_map
    
    def get_daily_actual_by_month(
        self,
        target_year: int,
        target_month: int
    ) -> Dict[date, float]:
        """
        Lấy actual revenue theo ngày cho toàn bộ tháng từ transactions
        Thay thế query từ actual_2026_day_staging
        Returns: dict {calendar_date: actual_amount}
        """
        query = f"""
            SELECT 
                toDate(t.created_at) as calendar_date,
                SUM(t.transaction_total) as actual_amount
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toYear(t.created_at) = {target_year}
              AND toMonth(t.created_at) = {target_month}
              AND t.status NOT IN ('Canceled', 'Cancel')
            GROUP BY calendar_date
            ORDER BY calendar_date
        """
        
        result = self.client.query(query)
        actual_map = {row[0]: float(row[1]) for row in result.result_rows}
        return actual_map
    
    def check_has_actual_for_month(
        self,
        target_year: int,
        target_month: int
    ) -> bool:
        """
        Kiểm tra có actual trong tháng từ transactions
        Thay thế query từ actual_2026_day_staging
        Returns: True nếu có actual, False nếu không có
        """
        query = f"""
            SELECT COUNT(*) as cnt
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toYear(t.created_at) = {target_year}
              AND toMonth(t.created_at) = {target_month}
              AND t.status NOT IN ('Canceled', 'Cancel')
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] > 0:
            return True
        return False
    
    # EOD (END OF DAY) RELATED QUERIES
    
    def get_hourly_revenue_percentage(self, days_back: int = 30) -> Dict[int, float]:
        """
        Tính % doanh thu theo giờ (0-23h) trong N ngày gần nhất từ transactions
        Returns: dict {hour: percentage} ví dụ {0: 0.05, 1: 0.10, ..., 23: 0.08}
        Tổng tất cả các % = 1.0
        """
        query = f"""
            SELECT 
                toHour(t.created_at) as hour,
                SUM(t.transaction_total) as hour_revenue
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toDate(t.created_at) >= today() - INTERVAL {days_back} DAY
              AND t.status NOT IN ('Canceled', 'Cancel')
            GROUP BY hour
            ORDER BY hour
        """
        
        result = self.client.query(query)
        
        # Tính tổng revenue của tất cả các giờ
        total_revenue = Decimal('0')
        hour_revenues = {}
        
        for row in result.result_rows:
            hour = int(row[0])
            revenue = Decimal(str(row[1]))
            hour_revenues[hour] = revenue
            total_revenue += revenue
        
        # Tính % cho từng giờ
        hourly_percentages = {}
        if total_revenue > 0:
            for hour in range(24):
                if hour in hour_revenues:
                    percentage = float(hour_revenues[hour] / total_revenue)
                    hourly_percentages[hour] = percentage
                else:
                    hourly_percentages[hour] = 0.0
        else:
            # Nếu không có data, set tất cả = 0
            for hour in range(24):
                hourly_percentages[hour] = 0.0
        
        return hourly_percentages
    
    def get_daily_actual_until_hour(self, target_date: date, until_hour: int) -> Decimal:
        """
        Lấy actual revenue từ đầu ngày đến < giờ hiện tại (0h00 đến <until_hour)
        Ví dụ: until_hour = 9 thì lấy actual từ 0h00 đến 8h59 (không bao gồm 9h00)
        Nếu chạy lúc 9h05 thì until_hour = 9, lấy actual từ 0h đến <9h
        Returns: tổng actual từ 0h00 đến <until_hour
        """
        query = f"""
            SELECT 
                SUM(t.transaction_total) as actual_amount
            FROM hskcdp.object_sql_transactions AS t FINAL
            WHERE toDate(t.created_at) = '{target_date}'
              AND toHour(t.created_at) < {until_hour}
              AND t.status NOT IN ('Canceled', 'Cancel')
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return Decimal(str(result.result_rows[0][0]))
        else:
            return Decimal('0')

