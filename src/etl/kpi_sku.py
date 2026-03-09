from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper
from src.utils.numeric_helper import safe_decimal, safe_float


class KPISKUCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_kpi_sku(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        # Lấy actual revenue theo sku, brand, channel và date
        actual_by_date = self.revenue_helper.get_actual_by_sku_brand_channel_and_date(
            target_year=target_year,
            target_month=target_month
        )
        
        query = f"""
            WITH brand_data AS (
                SELECT
                    calendar_date,
                    date_label,
                    channel,
                    brand_name,
                    kpi_brand_initial,
                    kpi_brand_adjustment
                FROM hskcdp.kpi_brand FINAL
                WHERE year = {target_year}
                    AND month = {target_month}
            ),
            brand_total_by_date AS (
                SELECT
                    calendar_date,
                    brand_name,
                    SUM(kpi_brand_initial) AS kpi_brand_total
                FROM brand_data
                GROUP BY calendar_date, brand_name
            ),
            ecom_products AS (
                SELECT
                    sku,
                    category_name
                FROM hskcdp.raw_ecom_products FINAL
            ),
            cross_join AS (
                SELECT
                    b.calendar_date AS calendar_date,
                    b.date_label AS date_label,
                    b.channel AS channel,
                    b.brand_name AS brand_name,
                    b.kpi_brand_initial AS kpi_brand_initial,
                    b.kpi_brand_adjustment AS kpi_brand_adjustment,
                    s.sku AS sku,
                    s.sku_classification AS sku_classification,
                    s.revenue_share_in_class AS revenue_share_in_class,
                    ep.category_name AS category_name
                FROM brand_data AS b
                INNER JOIN (
                    SELECT 
                        brand_name, 
                        sku, 
                        sku_classification, 
                        revenue_share_in_class 
                    FROM hskcdp.kpi_sku_metadata FINAL
                    WHERE year = {target_year}
                      AND month = {target_month}
                ) AS s 
                    ON s.brand_name = b.brand_name
                LEFT JOIN ecom_products AS ep 
                    ON s.sku = ep.sku
            ),
            brand_sku_stats AS (
                SELECT
                    brand_name,
                    COUNT(DISTINCT sku) AS total_sku_count,
                    COUNT(DISTINCT CASE WHEN sku_classification = 'Hero' THEN sku END) AS hero_count,
                    COUNT(DISTINCT CASE WHEN sku_classification = 'Core' THEN sku END) AS core_count,
                    COUNT(DISTINCT CASE WHEN sku_classification = 'Tail' THEN sku END) AS tail_count
                FROM cross_join
                GROUP BY brand_name
            ),
            adjusted_cross_join AS (
                SELECT
                    cj.*,
                    stats.total_sku_count,
                    stats.hero_count,
                    stats.core_count,
                    stats.tail_count,
                    CASE
                        WHEN stats.total_sku_count = 1 THEN 1.0
                        WHEN stats.hero_count > 0 AND stats.core_count = 0 AND stats.tail_count = 0 THEN 1.0
                        WHEN stats.hero_count > 0 AND stats.core_count = 0 AND stats.tail_count > 0 THEN 1.0
                        WHEN stats.hero_count > 0 AND stats.core_count > 0 THEN
                            CASE 
                                WHEN cj.sku_classification = 'Hero' THEN 0.85
                                WHEN cj.sku_classification = 'Core' THEN 0.15
                                ELSE 0.0
                            END
                        ELSE
                            CASE 
                                WHEN cj.sku_classification = 'Hero' THEN 0.85
                                WHEN cj.sku_classification = 'Core' THEN 0.15
                                ELSE 0.0
                            END
                    END AS group_percentage
                FROM cross_join cj
                INNER JOIN brand_sku_stats stats ON cj.brand_name = stats.brand_name
            )
            SELECT 
                sku.calendar_date, 
                sku.date_label,
                sku.channel,
                sku.brand_name,
                sku.kpi_brand_initial,
                sku.kpi_brand_adjustment,
                sku.sku, 
                sku.sku_classification, 
                sku.revenue_share_in_class,
                sku.hero_count,
                sku.core_count,
                bt.kpi_brand_total AS kpi_brand,
                sku.kpi_brand_initial * sku.group_percentage AS revenue_by_group_sku,
                (sku.revenue_share_in_class / 100.0) * sku.kpi_brand_initial * sku.group_percentage AS kpi_sku_initial, 
                sku.category_name AS category_name
            FROM adjusted_cross_join AS sku
            INNER JOIN brand_total_by_date bt 
                ON sku.calendar_date = bt.calendar_date
                AND sku.brand_name = bt.brand_name
            ORDER BY sku.calendar_date, sku.channel, sku.brand_name, sku.sku
        """
        
        result = self.client.query(query)
        
        results = []
        today = date.today()
        current_hour = datetime.now().hour
        
        # Lấy % revenue theo giờ và channel để tính forecast
        hourly_revenue_pct_by_channel = self.revenue_helper.get_hourly_revenue_percentage_by_channel(days_back=30)

        # Lấy giờ lớn nhất có transaction trong ngày hôm nay (nếu có)
        max_hour = self.revenue_helper.get_max_hour_from_transaction_details(target_year, target_month)
        if max_hour is not None:
            cutoff_hour = max_hour
        else:
            cutoff_hour = current_hour

        forecast_top_down_sku = self.revenue_helper.get_forecast_top_down_from_brand(
            target_year=target_year,
            target_month=target_month
        )
        
        # until_hour dùng cho get_daily_actual_until_hour: lấy từ 00:00 tới <until_hour
        until_hour = cutoff_hour + 1


        # Cache để lưu actual_by_sku cho mỗi date (hàm trả về tất cả channel)
        actual_by_sku_cache = {}
        
        for row in result.result_rows:
            calendar_date = row[0]
            date_label = str(row[1])
            channel = str(row[2])
            brand_name = str(row[3])
            
            kpi_brand_adjustment = safe_decimal(row[5])
            sku_name = str(row[6])
            sku_classification = str(row[7])
            revenue_share_in_class = safe_decimal(row[8])
            hero_count = int(row[9]) if row[9] is not None else 0
            core_count = int(row[10]) if row[10] is not None else 0
            kpi_sku_initial = safe_decimal(row[13])
            raw_category = str(row[14])
            if raw_category is None:
                category_name = ''
            else:
                cleaned = str(raw_category).strip()
                if cleaned.lower() == 'none':
                    category_name = ''
                else:
                    category_name = cleaned
            # Lấy actual revenue cho sku này
            actual = actual_by_date.get(calendar_date, {}).get(channel, {}).get(brand_name, {}).get(sku_name, 0.0)
            # Với Tail: kpi_sku_initial = 0 cho tất cả các ngày
            if sku_classification == 'Tail':
                kpi_sku_initial = Decimal('0')

            if calendar_date < today:
                kpi_sku_adjustment = actual
                gap = actual - float(kpi_sku_initial)
            else:
                gap = 0

                if hero_count > 0 and core_count > 0:
                    if sku_classification == 'Hero':
                        class_pct = Decimal('0.85')
                    elif sku_classification == 'Core':
                        class_pct = Decimal('0.15')
                    else:  # Tail
                        class_pct = Decimal('0')
                elif hero_count > 0 and core_count == 0:
                    if sku_classification == 'Hero':
                        class_pct = Decimal('1.00')
                    else:  # Core hoặc Tail
                        class_pct = Decimal('0')
                else:
                    if sku_classification == 'Hero':
                        class_pct = Decimal('0.85')
                    elif sku_classification == 'Core':
                        class_pct = Decimal('0.15')
                    else:  # Tail
                        class_pct = Decimal('0')
                
                rev_distribution = revenue_share_in_class / Decimal('100.0')

                if kpi_brand_adjustment is not None and float(kpi_brand_adjustment) > 0:
                    kpi_sku_adjustment = float(kpi_brand_adjustment) * float(rev_distribution) * float(class_pct)
                else:
                    kpi_sku_adjustment = 0.0
            
            # Tính forecast cho ngày hôm nay
            forecast = None
            if calendar_date < today:
                # Ngày quá khứ: forecast = actual
                forecast = actual
            elif calendar_date == today:
                cache_key = calendar_date
                if cache_key not in actual_by_sku_cache:
                    # Data return: {channel: {sku: actual}}
                    actual_by_sku_cache[cache_key] = self.revenue_helper.get_daily_actual_until_hour_by_sku(
                        target_date=calendar_date,
                        until_hour=until_hour
                    )
                
                actual_until_hour = Decimal('0')
                if channel in actual_by_sku_cache[cache_key] and sku_name in actual_by_sku_cache[cache_key][channel]:
                    actual_until_hour = Decimal(str(actual_by_sku_cache[cache_key][channel][sku_name]))
                
                # sum_check += actual_until_hour

                # Tính % revenue CỘNG DỒN từ 0h đến giờ cutoff cho channel này
                channel_pcts = hourly_revenue_pct_by_channel.get(channel, {})
                cumulative_pct = Decimal('0')
                for h in range(cutoff_hour + 1):
                    pct_h_raw = channel_pcts.get(h, 0.0)
                    cumulative_pct += Decimal(str(pct_h_raw))

                if cumulative_pct > Decimal('0'):
                    forecast = actual_until_hour / cumulative_pct
                else:
                    forecast = Decimal('0')
                    
            else:
                # forecast top-down
                if hero_count > 0 and core_count > 0:
                    if sku_classification == 'Hero':
                        class_pct = Decimal('0.85')
                    elif sku_classification == 'Core':
                        class_pct = Decimal('0.15')
                    else:  # Tail
                        class_pct = Decimal('0')
                elif hero_count > 0 and core_count == 0:
                    if sku_classification == 'Hero':
                        class_pct = Decimal('1.00')
                    else:  # Core hoặc Tail
                        class_pct = Decimal('0')
                else:
                    if sku_classification == 'Hero':
                        class_pct = Decimal('0.85')
                    elif sku_classification == 'Core':
                        class_pct = Decimal('0.15')
                    else:  # Tail
                        class_pct = Decimal('0')

                rev_distribution = revenue_share_in_class / Decimal("100")

                forecast = forecast_top_down_sku.get(calendar_date, {}).get(channel, {}).get(brand_name, Decimal("0")) * rev_distribution * class_pct
            
            results.append({
                'calendar_date': calendar_date,
                'year': calendar_date.year,
                'month': calendar_date.month,
                'date_label': date_label,
                'channel': channel,
                'brand_name': brand_name,
                'sku': sku_name,
                'sku_classification': sku_classification,
                'category_name': category_name,
                'revenue_share_in_class': revenue_share_in_class,
                'kpi_sku_initial': float(kpi_sku_initial),
                'actual': actual,
                'gap': gap,
                'kpi_sku_adjustment': kpi_sku_adjustment,
                'forecast': forecast
            })
        
        # Lấy SKU mới: xuất hiện lần đầu trong tháng hiện tại (giống logic brand)
        new_skus = self.revenue_helper.get_new_sku_this_month()

        # Lấy danh sách (brand_name, sku) có trong metadata kpi_sku_metadata
        skus_in_metadata = set()
        metadata_query = f"""
            SELECT DISTINCT brand_name, CAST(sku AS String) AS sku
            FROM hskcdp.kpi_sku_metadata FINAL
            WHERE year = {target_year}
              AND month = {target_month}
        """
        metadata_result = self.client.query(metadata_query)
        for row in metadata_result.result_rows:
            brand_name_meta = str(row[0])
            sku_meta = str(row[1])
            skus_in_metadata.add((brand_name_meta, sku_meta))

        # Lấy danh sách (brand_name, sku) có actual trong tháng target
        skus_with_actual = set()
        for calendar_date, channels in actual_by_date.items():
            for channel_name, brands in channels.items():
                for brand_name_actual, skus in brands.items():  
                    for sku_actual in skus.keys():
                        skus_with_actual.add((brand_name_actual, sku_actual))

        # SKU cần xử lý thêm: có actual nhưng không có trong metadata và không phải SKU mới
        skus_to_process = skus_with_actual - skus_in_metadata - new_skus

        # Gộp cả SKU mới và SKU cần xử lý thêm, xử lý chung giống logic SKU mới
        skus_for_new_logic = set()
        if new_skus:
            skus_for_new_logic |= new_skus
        if skus_to_process:
            skus_for_new_logic |= skus_to_process

        if skus_for_new_logic:
            new_sku_records = self.get_new_sku_records(
                target_year=target_year,
                target_month=target_month,
                new_skus=skus_for_new_logic,
                actual_by_date=actual_by_date,
                today=today,
                hourly_revenue_pct_by_channel=hourly_revenue_pct_by_channel,
                until_hour=until_hour,
                actual_by_sku_cache=actual_by_sku_cache
            )
            results.extend(new_sku_records)
        
        return results
    
    def get_new_sku_records(
        self,
        target_year: int,
        target_month: int,
        new_skus: set,
        actual_by_date: Dict,
        today: date,
        hourly_revenue_pct_by_channel: Dict,
        until_hour: int,
        actual_by_sku_cache: Dict
    ) -> List[Dict]:
        """
        Tạo records cho SKU mới (xuất hiện lần đầu trong tháng hiện tại)
        """
        results = []
        
        # Lấy category_name từ raw_ecom_products
        ecom_products_query = """
            SELECT sku, category_name
            FROM hskcdp.raw_ecom_products FINAL
        """
        ecom_result = self.client.query(ecom_products_query)
        category_by_sku = {}
        for row in ecom_result.result_rows:
            sku = str(row[0])
            raw_category = str(row[1]) if row[1] else None
            if raw_category is None:
                category_name = ''
            else:
                cleaned = str(raw_category).strip()
                if cleaned.lower() == 'none':
                    category_name = ''
                else:
                    category_name = cleaned
            category_by_sku[sku] = category_name
        
        for brand_name, sku_name in new_skus:
            # Lấy tất cả (calendar_date, channel) từ kpi_brand cho brand này
            brand_query = f"""
                SELECT DISTINCT calendar_date, date_label, channel
                FROM hskcdp.kpi_brand FINAL
                WHERE year = {target_year}
                  AND month = {target_month}
                  AND brand_name = '{brand_name.replace("'", "''")}'
                ORDER BY calendar_date, channel
            """
            brand_result = self.client.query(brand_query)
            
            for row in brand_result.result_rows:
                calendar_date = row[0]
                date_label = str(row[1])
                channel = str(row[2])
                
                # Lấy actual revenue
                actual = Decimal(str(actual_by_date.get(calendar_date, {}).get(channel, {}).get(brand_name, {}).get(sku_name, 0.0)))
                
                # Logic cho SKU mới: kpi_sku_initial = 0, không tạo record cho ngày tương lai
                kpi_sku_initial = Decimal('0')

                if calendar_date < today:
                    kpi_sku_adjustment = actual
                else:
                    kpi_sku_adjustment = None

                gap = actual
                sku_classification = 'New'
                revenue_share_in_class = Decimal('0')
                category_name = category_by_sku.get(sku_name, '')
                
                # Tính forecast
                forecast = None
                if calendar_date < today:
                    # Ngày quá khứ: forecast = actual
                    forecast = actual
                elif calendar_date == today:
                    cache_key = calendar_date
                    if cache_key not in actual_by_sku_cache:
                        actual_by_sku_cache[cache_key] = self.revenue_helper.get_daily_actual_until_hour_by_sku(
                            target_date=calendar_date,
                            until_hour=until_hour
                        )
                    
                    actual_until_hour = Decimal('0')
                    if channel in actual_by_sku_cache[cache_key] and sku_name in actual_by_sku_cache[cache_key][channel]:
                        actual_until_hour = Decimal(str(actual_by_sku_cache[cache_key][channel][sku_name]))
                    
                    # Tính % revenue CỘNG DỒN từ 0h đến giờ cutoff cho channel này
                    channel_pcts = hourly_revenue_pct_by_channel.get(channel, {})
                    cumulative_pct = Decimal('0')
                    # until_hour = cutoff_hour + 1, nên cutoff_hour = until_hour - 1
                    cutoff_hour_for_forecast = until_hour - 1
                    for h in range(cutoff_hour_for_forecast + 1):
                        pct_h_raw = channel_pcts.get(h, 0.0)
                        cumulative_pct += Decimal(str(pct_h_raw))
                    
                    if cumulative_pct > Decimal('0'):
                        forecast = actual_until_hour / cumulative_pct
                    else:
                        forecast = Decimal('0')
                else:
                    forecast = Decimal('0')
                
                results.append({
                    'calendar_date': calendar_date,
                    'year': calendar_date.year,
                    'month': calendar_date.month,
                    'date_label': date_label,
                    'channel': channel,
                    'brand_name': brand_name,
                    'sku': sku_name,
                    'sku_classification': sku_classification,
                    'category_name': category_name,
                    'revenue_share_in_class': revenue_share_in_class,
                    'kpi_sku_initial': float(kpi_sku_initial),
                    'actual': actual,
                    'gap': gap,
                    'kpi_sku_adjustment': kpi_sku_adjustment,
                    'forecast': forecast
                })
        
        return results
    
    def save_kpi_sku(self, kpi_sku_data: List[Dict]) -> None:
        if not kpi_sku_data:
            return
        
        now = datetime.now()
        
        data = []
        for row in kpi_sku_data:
            
            data.append([
                row['calendar_date'],
                row['year'],
                row['month'],
                row['date_label'],
                row['channel'],
                row['brand_name'],
                row['sku'],
                row['sku_classification'],
                row['category_name'],
                safe_float(row['revenue_share_in_class']),
                safe_float(row['kpi_sku_initial']),
                safe_float(row.get('actual')),
                safe_float(row.get('gap')),
                safe_float(row.get('kpi_sku_adjustment')),
                safe_float(row.get('forecast')),
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'date_label',
            'channel', 'brand_name', 'sku', 'sku_classification', 'category_name',
            'revenue_share_in_class',
            'kpi_sku_initial',
            'actual', 'gap', 'kpi_sku_adjustment', 'forecast',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_sku", data, column_names=columns)
    
    def calculate_and_save_kpi_sku(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        kpi_sku_data = self.calculate_kpi_sku(
            target_year=target_year,
            target_month=target_month
        )
        
        self.save_kpi_sku(kpi_sku_data)
        
        return kpi_sku_data


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPISKUCalculator(constants)
    
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
    
    print(f"Calculating kpi_sku for month {target_month}/{target_year}...")
    kpi_sku_data = calculator.calculate_and_save_kpi_sku(
        target_year=target_year,
        target_month=target_month
    )
    
    print(f"Successfully saved {len(kpi_sku_data)} kpi_sku records")