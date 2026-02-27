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
                    FROM hskcdp.dim_sku 
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
        
        # until_hour dùng cho get_daily_actual_until_hour: lấy từ 00:00 tới <until_hour
        until_hour = cutoff_hour + 1


        # Cache để lưu actual_by_sku cho mỗi date (hàm trả về tất cả channel)
        actual_by_sku_cache = {}
        
        for row in result.result_rows:
            calendar_date = row[0]
            date_label = str(row[1])
            channel = str(row[2])
            brand_name = str(row[3])
            
            kpi_brand_initial = safe_decimal(row[4])
            kpi_brand_adjustment = safe_decimal(row[5])
            sku_name = str(row[6])
            sku_classification = str(row[7])
            revenue_share_in_class = safe_decimal(row[8])
            hero_count = int(row[9]) if row[9] is not None else 0
            core_count = int(row[10]) if row[10] is not None else 0
            kpi_brand = safe_decimal(row[11])
            revenue_by_group_sku = safe_decimal(row[12])
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
                revenue_by_group_sku = Decimal('0')

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
            
            # Giữ các cột cũ để backward compatibility
            kpi_day_channel_brand = float(kpi_brand_initial)
            
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
                forecast = Decimal('0')
            
            results.append({
                'calendar_date': calendar_date,
                'date_label': date_label,
                'channel': channel,
                'brand_name': brand_name,
                'sku': sku_name,
                'sku_classification': sku_classification,
                'category_name': category_name,
                'revenue_share_in_class': revenue_share_in_class,
                'kpi_day_channel_brand': kpi_day_channel_brand,
                'kpi_brand': float(kpi_brand),
                'revenue_by_group_sku': float(revenue_by_group_sku),
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
                row['date_label'],
                row['channel'],
                row['brand_name'],
                row['sku'],
                row['sku_classification'],
                row['category_name'],
                safe_float(row['revenue_share_in_class']),
                safe_float(row['kpi_day_channel_brand']),
                safe_float(row['kpi_brand']),
                safe_float(row['revenue_by_group_sku']),
                safe_float(row['kpi_sku_initial']),
                safe_float(row.get('actual')),
                safe_float(row.get('gap')),
                safe_float(row.get('kpi_sku_adjustment')),
                safe_float(row.get('forecast')),
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'date_label',
            'channel', 'brand_name', 'sku', 'sku_classification', 'category_name',
            'revenue_share_in_class', 'kpi_day_channel_brand', 'kpi_brand',
            'revenue_by_group_sku', 'kpi_sku_initial',
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