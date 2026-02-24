from decimal import Decimal
from datetime import date, timedelta, datetime
from typing import Dict, Set, List
from src.utils.clickhouse_client import get_client


class RevenueQueryHelper:
    def __init__(self):
        self.client = get_client()

    # KPI MONTH RELATED QUERIES
    
    def get_avg_rev_normal_day_30_days(self) -> Decimal:
        query = f"""
            SELECT
                AVG(daily_revenue) AS avg_rev_normal_day
            FROM (
                SELECT
                    toDate(t.created_at) as calendar_date,
                    SUM(t.total_amount) AS daily_revenue
                FROM hskcdp.object_sql_transaction_details AS t FINAL
                INNER JOIN hskcdp.dim_date d
                    ON toDate(t.created_at) = d.calendar_date
                WHERE d.date_label = 'Normal day'
                    AND toDate(t.created_at) >= today() - 30
                    AND t.status NOT IN ('Canceled', 'Cancel')
                GROUP BY calendar_date
            )
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return Decimal(str(result.result_rows[0][0]))
        else:
            raise ValueError("Cannot calculate avg rev normal day: no data found")
    
    def get_daily_actual_sum(self, target_year: int, target_month: int) -> Decimal:
        query = f"""
            SELECT 
                SUM(COALESCE(total_amount, 0)) as sum_actual
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toYear(created_at) = {target_year}
              AND toMonth(created_at) = {target_month}
              AND toDate(created_at) < today()
              AND status NOT IN ('Canceled', 'Cancel')
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return Decimal(str(result.result_rows[0][0]))
        else:
            return Decimal('0')

    def get_daily_actual_sum_for_eom_calculation(self, target_year: int, target_month: int) -> Decimal:
        query = f"""
            SELECT 
                SUM(COALESCE(total_amount, 0)) as sum_actual
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toYear(created_at) = {target_year}
              AND toMonth(created_at) = {target_month}
              AND toDate(created_at) < today()
              AND status NOT IN ('Canceled', 'Cancel')
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return Decimal(str(result.result_rows[0][0]))
        else:
            return Decimal('0')
    
    def get_actual_dates(self, target_year: int, target_month: int) -> Set[date]:
        query = f"""
            SELECT 
                DISTINCT toDate(created_at) as calendar_date
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toYear(created_at) = {target_year}
                AND toMonth(created_at) = {target_month}
                AND status NOT IN ('Canceled', 'Cancel')
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
        if not actual_dates:
            return {}
        
        dates_str = ','.join([f"'{d}'" for d in actual_dates])
        
        query = f"""
            SELECT 
                d.date_label,
                COUNT(DISTINCT toDate(t.created_at)) as so_ngay
            FROM hskcdp.object_sql_transaction_details AS t FINAL
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

    def get_monthly_actual(self, target_year: int) -> Dict[int, Decimal]:
        query = f"""
            SELECT 
                toMonth(created_at) as month,
                SUM(COALESCE(total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toYear(created_at) = {target_year}
              AND toDate(created_at) < today()
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY month
            ORDER BY month
        """
        
        result = self.client.query(query)
        actuals_month = {row[0]: Decimal(row[1]) for row in result.result_rows}
        return actuals_month
    
    # KPI DAY METADATA RELATED QUERIES (HISTORICAL REVENUE QUERIES)

    def get_historical_revenue_by_date_label(
        self,
        date_labels: List[str]
    ) -> Dict[str, Dict]:
        date_labels_str = ','.join([f"'{dl}'" for dl in date_labels])
        
        query = f"""
            SELECT 
                a.date_label,
                AVG(a.daily_revenue) as avg_total,
                COUNT(DISTINCT a.calendar_date) as so_ngay_historical
            FROM (
                SELECT 
                    d.calendar_date,
                    d.date_label,
                    SUM(t.total_amount) as daily_revenue
                FROM hskcdp.object_sql_transaction_details AS t FINAL
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
            avg_total = Decimal(row[1])
            so_ngay_historical = int(row[2])
            historical_data[date_label] = {
                'avg_total': avg_total,
                'so_ngay_historical': so_ngay_historical
            }
        
        return historical_data


    # KPI DAY RELATED QUERIES
    
    def get_daily_actual_by_dates(self, calendar_dates: List[date]) -> Dict[date, Decimal]:
        if not calendar_dates:
            return {}
        
        dates_str = ','.join([f"'{d}'" for d in calendar_dates])
        
        query = f"""
            SELECT 
                toDate(created_at) as calendar_date,
                SUM(COALESCE(total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toDate(created_at) IN ({dates_str})
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY calendar_date
        """
        
        result = self.client.query(query)
        actual_map = {row[0]: Decimal(row[1]) for row in result.result_rows}
        return actual_map
    
    def get_daily_actual_by_month(
        self,
        target_year: int,
        target_month: int
    ) -> Dict[date, Decimal]:
        query = f"""
            SELECT 
                toDate(created_at) as calendar_date,
                SUM(COALESCE(total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toYear(created_at) = {target_year}
              AND toMonth(created_at) = {target_month}
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY calendar_date
            ORDER BY calendar_date
        """
        
        result = self.client.query(query)
        actual_map = {row[0]: Decimal(row[1]) for row in result.result_rows}
        return actual_map
    
    # EOD (END OF DAY) RELATED QUERIES
    
    def get_hourly_revenue_percentage(self, days_back: int = 30) -> Dict[int, Decimal]:
        query = f"""
            SELECT 
                toHour(created_at) as hour,
                SUM(COALESCE(total_amount, 0)) as hour_revenue
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toDate(created_at) >= today() - INTERVAL {days_back} DAY
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY hour
            ORDER BY hour
        """
        
        result = self.client.query(query)
        
        total_revenue = Decimal('0')
        hour_revenues = {}
        
        for row in result.result_rows:
            hour = int(row[0])
            revenue = Decimal(str(row[1]))
            hour_revenues[hour] = revenue
            total_revenue += revenue
        
        hourly_percentages = {}
        if total_revenue > 0:
            for hour in range(24):
                if hour in hour_revenues:
                    percentage = Decimal(hour_revenues[hour] / total_revenue)
                    hourly_percentages[hour] = percentage
                else:
                    hourly_percentages[hour] = 0.0
        else:
            for hour in range(24):
                hourly_percentages[hour] = 0.0
        
        return hourly_percentages
    
    def get_daily_actual_until_hour(self, target_date: date, until_hour: int) -> Decimal:
        query = f"""
            SELECT 
                SUM(COALESCE(total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toDate(created_at) = '{target_date}'
              AND toHour(created_at) < {until_hour}
              AND status NOT IN ('Canceled', 'Cancel')
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return Decimal(str(result.result_rows[0][0]))
        else:
            return Decimal('0')

    def get_hourly_revenue_percentage_by_channel(self, days_back: int = 30) -> Dict[str, Dict[int, float]]:

        query = f"""
            SELECT 
                toHour(created_at) as hour,
                platform,
                SUM(COALESCE(total_amount, 0)) as hour_revenue
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toDate(created_at) BETWEEN today() - INTERVAL {days_back} DAY AND today() - INTERVAL 1 DAY
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY hour, platform 
            ORDER BY hour, platform
        """
        
        result = self.client.query(query)
        
        # Map platform về channel và tính tổng revenue của tất cả các giờ theo từng channel
        channel_hour_revenues = {}
        channel_totals = {}
        
        for row in result.result_rows:
            hour = int(row[0])
            platform = str(row[1])
            revenue = Decimal(str(row[2]))
            
            # Map platform về channel
            if platform == 'ONLINE_HASAKI':
                channel = 'ONLINE_HASAKI'
            elif platform == 'OFFLINE_HASAKI':
                channel = 'OFFLINE_HASAKI'
            else:
                channel = 'ECOM'
            
            if channel not in channel_hour_revenues:
                channel_hour_revenues[channel] = {}
                channel_totals[channel] = Decimal('0')
            
            if hour not in channel_hour_revenues[channel]:
                channel_hour_revenues[channel][hour] = Decimal('0')
            
            channel_hour_revenues[channel][hour] += revenue
            channel_totals[channel] += revenue
        
        # Tính % cho từng giờ theo từng channel
        channel_hourly_percentages = {}
        
        for channel in channel_hour_revenues:
            channel_hourly_percentages[channel] = {}
            total_revenue = channel_totals[channel]
            
            if total_revenue > 0:
                for hour in range(24):
                    if hour in channel_hour_revenues[channel]:
                        percentage = float(channel_hour_revenues[channel][hour] / total_revenue)
                        channel_hourly_percentages[channel][hour] = percentage
                    else:
                        channel_hourly_percentages[channel][hour] = 0.0
            else:
                # Nếu không có data, set tất cả = 0
                for hour in range(24):
                    channel_hourly_percentages[channel][hour] = 0.0
        
        return channel_hourly_percentages

    def get_daily_actual_until_hour_by_sku(
        self, 
        target_date: date, 
        until_hour: int
    ) -> Dict[str, Dict[str, Decimal]]:
        """
        Returns: dict {channel: {sku: actual_amount}} - tổng actual của mỗi SKU từ 0h00 đến <until_hour theo từng channel
        Platform trong DB thực chất là channel (ONLINE_HASAKI, OFFLINE_HASAKI, ECOM)
        """
        query = f"""
            SELECT 
                CAST(sku AS String) AS sku,
                platform,
                SUM(COALESCE(total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toDate(created_at) = '{target_date}'
              AND toHour(created_at) < {until_hour}
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY sku, platform
        """
        result = self.client.query(query)
        channel_sku_actuals = {}
        for row in result.result_rows:
            sku = str(row[0])
            channel = str(row[1])  # platform trong DB = channel
            actual_amount = Decimal(str(row[2]))
            
            if channel not in channel_sku_actuals:
                channel_sku_actuals[channel] = {}
            
            channel_sku_actuals[channel][sku] = actual_amount
        
        return channel_sku_actuals        

    # KPI CHANNEL METADATA RELATED QUERIES
    
    def get_total_revenue_by_date_label_last_3_months(
        self,
        date_labels: List[str]
    ) -> Dict[str, Decimal]:
        date_labels_str = ','.join([f"'{dl}'" for dl in date_labels])
        
        query = f"""
            SELECT 
                d.priority_label AS date_label, 
                SUM(t.total_amount) as total_revenue 
            FROM hskcdp.object_sql_transaction_details AS t FINAL
            INNER JOIN hskcdp.dim_date d
                ON toDate(t.created_at) = d.calendar_date
            WHERE toDate(t.created_at) >= today() - INTERVAL 3 MONTH
              AND d.priority_label IN ({date_labels_str})
              AND t.status NOT IN ('Canceled', 'Cancel')
              AND (toMonth(t.created_at), toDayOfMonth(t.created_at)) NOT IN (
                    (6,6), (9,9), (11,11), (12,12)
              )
            GROUP BY d.priority_label
        """
        
        result = self.client.query(query)
        total_revenue_by_label = {row[0]: Decimal(row[1]) for row in result.result_rows}
        return total_revenue_by_label
    
    def get_revenue_by_date_label_and_channel_from_platform_last_3_months(
        self,
        date_labels: List[str]
    ) -> Dict[str, Dict[str, Decimal]]:
        date_labels_str = ','.join([f"'{dl}'" for dl in date_labels])
        
        query = f"""
            SELECT 
                d.priority_label AS date_label,
                CASE 
                    WHEN t.platform = 'ONLINE_HASAKI' THEN 'ONLINE_HASAKI'
                    WHEN t.platform = 'OFFLINE_HASAKI' THEN 'OFFLINE_HASAKI'
                    ELSE 'ECOM'
                END as channel,
                SUM(t.total_amount) as revenue
            FROM hskcdp.object_sql_transaction_details AS t FINAL
            INNER JOIN hskcdp.dim_date d
                ON toDate(t.created_at) = d.calendar_date
            WHERE toDate(t.created_at) >= today() - INTERVAL 3 MONTH
              AND d.priority_label IN ({date_labels_str})
              AND t.status NOT IN ('Canceled', 'Cancel')
              AND (toMonth(t.created_at), toDayOfMonth(t.created_at)) NOT IN (
                    (6,6), (9,9), (11,11), (12,12)
              )
            GROUP BY d.priority_label, channel
        """
        
        result = self.client.query(query)
        
        channel_revenue = {}
        for row in result.result_rows:
            date_label = row[0]
            channel = row[1]
            revenue = Decimal(row[2])
            
            if date_label not in channel_revenue:
                channel_revenue[date_label] = {}
            
            channel_revenue[date_label][channel] = revenue
        
        return channel_revenue
    
    def get_dim_dates_for_month_excluding_double_days(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        query = f"""
            SELECT 
                calendar_date,
                year,
                month,
                day,
                priority_label AS date_label
            FROM dim_date
            WHERE year = {target_year}
              AND month = {target_month}
              AND NOT (
                  (month = 6 AND day BETWEEN 5 AND 7) OR
                  (month = 9 AND day BETWEEN 8 AND 10) OR
                  (month = 11 AND day BETWEEN 10 AND 12) OR
                  (month = 12 AND day BETWEEN 11 AND 13)
              )
            ORDER BY calendar_date
        """
        
        result = self.client.query(query)
        
        dim_dates = []
        for row in result.result_rows:
            dim_dates.append({
                'calendar_date': row[0],
                'year': int(row[1]),
                'month': int(row[2]),
                'day': int(row[3]),
                'date_label': str(row[4])
            })
        
        return dim_dates
    
    # KPI CHANNEL RELATED QUERIES
    
    def get_kpi_day_with_channel_metadata(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        query = f"""
            SELECT 
                kd.calendar_date,
                kd.year,
                kd.month,
                kd.day,
                kd.date_label,
                md.channel,
                md.rev_pct_adjustment,
                kd.kpi_day_initial
            FROM (SELECT * FROM hskcdp.kpi_day FINAL) AS kd
            INNER JOIN (SELECT * FROM hskcdp.kpi_channel_metadata FINAL) AS md
                ON kd.calendar_date = md.calendar_date
                AND kd.year = md.year
                AND kd.month = md.month
                AND kd.day = md.day
                AND kd.date_label = md.date_label
            WHERE kd.year = {target_year}
              AND kd.month = {target_month}
              AND NOT (
                  (kd.month = 6 AND kd.day BETWEEN 5 AND 7) OR
                  (kd.month = 9 AND kd.day BETWEEN 8 AND 10) OR
                  (kd.month = 11 AND kd.day BETWEEN 10 AND 12) OR
                  (kd.month = 12 AND kd.day BETWEEN 11 AND 13)
              )
            ORDER BY kd.calendar_date, md.channel
        """
        
        result = self.client.query(query)
        
        kpi_day_channel_data = []
        for row in result.result_rows:
            kpi_day_channel_data.append({
                'calendar_date': row[0],
                'year': int(row[1]),
                'month': int(row[2]),
                'day': int(row[3]),
                'date_label': str(row[4]),
                'channel': str(row[5]),
                'rev_pct_adjustment': Decimal(str(row[6])),
                'kpi_day_initial': Decimal(str(row[7]))
            })
        
        return kpi_day_channel_data
    
    def get_actual_by_channel_and_date(
        self,
        target_year: int,
        target_month: int
    ) -> Dict[date, Dict[str, Decimal]]:
        query = f"""
            SELECT 
                toDate(created_at) as calendar_date,
                CASE 
                    WHEN platform = 'ONLINE_HASAKI' THEN 'ONLINE_HASAKI'
                    WHEN platform = 'OFFLINE_HASAKI' THEN 'OFFLINE_HASAKI'
                    ELSE 'ECOM'
                END as channel,
                SUM(COALESCE(total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toYear(created_at) = {target_year}
              AND toMonth(created_at) = {target_month}
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY calendar_date, channel
        """
        
        result = self.client.query(query)
        
        actual_by_date = {}
        for row in result.result_rows:
            calendar_date = row[0]
            channel = row[1]
            actual_amount = Decimal(row[2])
            
            if calendar_date not in actual_by_date:
                actual_by_date[calendar_date] = {}
            
            actual_by_date[calendar_date][channel] = actual_amount
        
        return actual_by_date
    
    def get_kpi_day_adjustment_by_date(
        self,
        target_year: int,
        target_month: int
    ) -> Dict[date, Decimal]:
        query = f"""
            SELECT 
                calendar_date,
                kpi_day_adjustment
            FROM hskcdp.kpi_day FINAL
            WHERE year = {target_year}
              AND month = {target_month}
            ORDER BY calendar_date
        """
        
        result = self.client.query(query)
        
        kpi_day_adjustment_by_date = {}
        for row in result.result_rows:
            calendar_date = row[0]
            kpi_day_adjustment = row[1]
            
            if kpi_day_adjustment is not None:
                kpi_day_adjustment_by_date[calendar_date] = Decimal(str(kpi_day_adjustment))
            else:
                kpi_day_adjustment_by_date[calendar_date] = None
        
        return kpi_day_adjustment_by_date
    
    # KPI BRAND METADATA RELATED QUERIES
    
    def get_revenue_by_brand_last_3_months(self) -> Dict[str, float]:
        """
        Lấy revenue theo brand từ object_sql_transaction_details (3 tháng gần nhất)
        Returns: dict {brand_name: revenue}
        """
        query = f"""
            SELECT 
                brand_name,
                SUM(COALESCE(total_amount, 0)) as revenue
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toDate(created_at) >= today() - INTERVAL 3 MONTH
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY brand_name
            HAVING SUM(COALESCE(total_amount, 0)) > 0
            ORDER BY brand_name
        """
        
        result = self.client.query(query)
        
        revenue_by_brand = {}
        for row in result.result_rows:
            brand_name = str(row[0])
            revenue = float(row[1])
            revenue_by_brand[brand_name] = revenue
        
        return revenue_by_brand
    
    # KPI BRAND RELATED QUERIES
    
    def get_kpi_brand_with_brand_metadata(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        """
        Lấy kpi_channel_initial và per_of_rev_by_brand_adj từ kpi_channel
        Tính per_of_rev_by_brand_adj theo từng channel riêng biệt từ 3 tháng gần nhất
        Returns: list of dicts với keys: calendar_date, year, month, day, date_label, 
                 channel, brand_name, per_of_rev_by_brand_adj, kpi_channel_initial
        """
        query = f"""
            WITH rev AS (
                SELECT
                    CASE
                        WHEN platform = 'ONLINE_HASAKI' THEN 'ONLINE_HASAKI'
                        WHEN platform = 'OFFLINE_HASAKI' THEN 'OFFLINE_HASAKI'
                        ELSE 'ECOM'
                    END AS channel,
                    brand_name,
                    SUM(COALESCE(total_amount, 0)) AS revenue
                FROM hskcdp.object_sql_transaction_details FINAL
                WHERE toDate(created_at) >= today() - INTERVAL 3 MONTH
                  AND status NOT IN ('Canceled', 'Cancel')
                GROUP BY channel, brand_name
            ),

            totals AS (
                SELECT SUM(revenue) AS total_revenue
                FROM rev
            ),

            brand_pct AS (
                SELECT
                    r.channel,
                    r.brand_name,
                    r.revenue / nullIf(t.total_revenue, 0) AS per_of_rev_by_brand_adj
                FROM rev r
                CROSS JOIN totals t
            )
            SELECT
                c.calendar_date,
                c.year,
                c.month,
                c.day,
                c.date_label,
                c.channel,
                b.brand_name,
                b.per_of_rev_by_brand_adj,
                c.kpi_channel_initial
            FROM (SELECT * FROM hskcdp.kpi_channel FINAL) AS c 
            CROSS JOIN (
                SELECT 
                    brand_name,
                    per_of_rev_by_brand_adj
                FROM hskcdp.kpi_brand_metadata FINAL
                WHERE month = {target_month}
            ) AS b
            WHERE c.year = {target_year}
              AND c.month = {target_month}
              AND NOT (
                  (c.month = 6 AND c.day BETWEEN 5 AND 7) OR
                  (c.month = 9 AND c.day BETWEEN 8 AND 10) OR
                  (c.month = 11 AND c.day BETWEEN 10 AND 12) OR
                  (c.month = 12 AND c.day BETWEEN 11 AND 13)
              )
            ORDER BY c.calendar_date, c.channel, b.brand_name
        """
        
        result = self.client.query(query)
        
        kpi_brand_data = []
        for row in result.result_rows:
            kpi_brand_data.append({
                'calendar_date': row[0],
                'year': int(row[1]),
                'month': int(row[2]),
                'day': int(row[3]),
                'date_label': str(row[4]),
                'channel': str(row[5]),
                'brand_name': str(row[6]),
                'per_of_rev_by_brand_adj': Decimal(str(row[7])),
                'kpi_channel_initial': Decimal(str(row[8]))
            })
        
        return kpi_brand_data
    
    def get_actual_by_brand_channel_and_date(
        self,
        target_year: int,
        target_month: int
    ) -> Dict[date, Dict[str, Dict[str, float]]]:
        """
        Lấy actual revenue theo brand, channel và date từ object_sql_transaction_details
        Platform được map thành channel: ONLINE_HASAKI, OFFLINE_HASAKI, ECOM
        Returns: dict {calendar_date: {channel: {brand_name: actual_amount}}}
        """
        query = f"""
            SELECT 
                toDate(created_at) as calendar_date,
                CASE 
                    WHEN platform = 'ONLINE_HASAKI' THEN 'ONLINE_HASAKI'
                    WHEN platform = 'OFFLINE_HASAKI' THEN 'OFFLINE_HASAKI'
                    ELSE 'ECOM'
                END as channel,
                brand_name,
                SUM(COALESCE(total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toYear(created_at) = {target_year}
              AND toMonth(created_at) = {target_month}
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY calendar_date, channel, brand_name
        """
        
        result = self.client.query(query)
        
        actual_by_date = {}
        for row in result.result_rows:
            calendar_date = row[0]
            channel = row[1]
            brand_name = str(row[2])
            actual_amount = float(row[3])
            
            if calendar_date not in actual_by_date:
                actual_by_date[calendar_date] = {}
            
            if channel not in actual_by_date[calendar_date]:
                actual_by_date[calendar_date][channel] = {}
            
            actual_by_date[calendar_date][channel][brand_name] = actual_amount
        
        return actual_by_date
    
    def get_kpi_day_channel_adjustment_by_date_and_channel(
        self,
        target_year: int,
        target_month: int
    ) -> Dict[date, Dict[str, Decimal]]:
        """
        Lấy kpi_channel_adjustment từ kpi_channel theo date và channel
        Returns: dict {calendar_date: {channel: kpi_channel_adjustment}}
        """
        query = f"""
            SELECT 
                calendar_date,
                channel,
                kpi_channel_adjustment
            FROM hskcdp.kpi_channel FINAL
            WHERE year = {target_year}
              AND month = {target_month}
            ORDER BY calendar_date, channel
        """
        
        result = self.client.query(query)
        
        kpi_day_channel_adjustment_by_date = {}
        for row in result.result_rows:
            calendar_date = row[0]
            channel = str(row[1])
            kpi_channel_adjustment = row[2]
            
            if calendar_date not in kpi_day_channel_adjustment_by_date:
                kpi_day_channel_adjustment_by_date[calendar_date] = {}
            
            if kpi_channel_adjustment is not None:
                kpi_day_channel_adjustment_by_date[calendar_date][channel] = Decimal(str(kpi_channel_adjustment))
            else:
                kpi_day_channel_adjustment_by_date[calendar_date][channel] = None
        
        return kpi_day_channel_adjustment_by_date
    
    # KPI SKU RELATED QUERIES
    
    def get_actual_by_sku_brand_channel_and_date(
        self,
        target_year: int,
        target_month: int
    ) -> Dict[date, Dict[str, Dict[str, Dict[str, float]]]]:
        """
        Lấy actual revenue theo sku, brand, channel và date từ object_sql_transaction_details
        Platform được map thành channel: ONLINE_HASAKI, OFFLINE_HASAKI, ECOM
        Returns: dict {calendar_date: {channel: {brand_name: {sku: actual_amount}}}}
        """
        query = f"""
            SELECT 
                toDate(created_at) as calendar_date,
                CASE 
                    WHEN platform = 'ONLINE_HASAKI' THEN 'ONLINE_HASAKI'
                    WHEN platform = 'OFFLINE_HASAKI' THEN 'OFFLINE_HASAKI'
                    ELSE 'ECOM'
                END as channel,
                brand_name,
                CAST(sku AS String) AS sku,
                SUM(COALESCE(total_amount, 0)) as actual_amount
            FROM hskcdp.object_sql_transaction_details FINAL
            WHERE toYear(created_at) = {target_year}
              AND toMonth(created_at) = {target_month}
              AND status NOT IN ('Canceled', 'Cancel')
            GROUP BY calendar_date, channel, brand_name, sku
        """
        
        result = self.client.query(query)
        
        actual_by_date = {}
        for row in result.result_rows:
            calendar_date = row[0]
            channel = row[1]
            brand_name = str(row[2])
            sku = str(row[3])
            actual_amount = float(row[4])
            if calendar_date not in actual_by_date:
                actual_by_date[calendar_date] = {}
            if channel not in actual_by_date[calendar_date]:
                actual_by_date[calendar_date][channel] = {}
            if brand_name not in actual_by_date[calendar_date][channel]:
                actual_by_date[calendar_date][channel][brand_name] = {}
            
            actual_by_date[calendar_date][channel][brand_name][sku] = actual_amount
        
        return actual_by_date