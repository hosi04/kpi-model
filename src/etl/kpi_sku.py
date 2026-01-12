from decimal import Decimal
from datetime import datetime
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class KPISKUCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def calculate_kpi_sku(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        
        query = f"""
            WITH cross_join AS (
                SELECT
                    brand.calendar_date,
                    brand.date_label,
                    brand.channel,
                    brand.brand_name,
                    brand.kpi_day_channel_brand,
                    sku.sku,
                    sku.sku_classification,
                    sku.revenue_share_in_class
                FROM hskcdp.kpi_day_channel_brand AS brand
                INNER JOIN (
                    SELECT 
                        brand_name, 
                        sku, 
                        sku_classification,
                        revenue_share_in_class
                    FROM hskcdp.dim_sku 
                    WHERE sku_classification != 'Tail'
                ) AS sku 
                ON sku.brand_name = brand.brand_name
                WHERE brand.year = {target_year}
                  AND brand.month = {target_month}
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
                sku.kpi_day_channel_brand, 
                sku.sku, 
                sku.sku_classification,
                sku.revenue_share_in_class,
                sku.kpi_day_channel_brand AS kpi_brand,
                sku.kpi_day_channel_brand * sku.group_percentage AS revenue_by_group_sku,
                (sku.revenue_share_in_class / 100.0) * sku.kpi_day_channel_brand * sku.group_percentage AS kpi_sku_initial
            FROM adjusted_cross_join AS sku
            ORDER BY sku.calendar_date, sku.channel, sku.brand_name, sku.sku
        """
        
        result = self.client.query(query)
        
        results = []
        for row in result.result_rows:
            results.append({
                'calendar_date': row[0],
                'date_label': str(row[1]),
                'channel': str(row[2]),
                'brand_name': str(row[3]),
                'kpi_day_channel_brand': Decimal(str(row[4])),
                'sku': str(row[5]),
                'sku_classification': str(row[6]),
                'revenue_share_in_class': Decimal(str(row[7])) if row[7] is not None else Decimal('0'),
                'kpi_brand': Decimal(str(row[8])),
                'revenue_by_group_sku': Decimal(str(row[9])),
                'kpi_sku_initial': Decimal(str(row[10]))
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
                float(row['revenue_share_in_class']),
                float(row['kpi_day_channel_brand']),
                float(row['kpi_brand']),
                float(row['revenue_by_group_sku']),
                float(row['kpi_sku_initial']),
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'date_label',
            'channel', 'brand_name', 'sku', 'sku_classification',
            'revenue_share_in_class', 'kpi_day_channel_brand', 'kpi_brand',
            'revenue_by_group_sku', 'kpi_sku_initial',
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
    constants = Constants()
    calculator = KPISKUCalculator(constants)
    
    print("Calculating kpi_sku for month 1/2026...")
    kpi_sku_data = calculator.calculate_and_save_kpi_sku(
        target_year=2026,
        target_month=1
    )
    
    print(f"Successfully saved {len(kpi_sku_data)} kpi_sku records")