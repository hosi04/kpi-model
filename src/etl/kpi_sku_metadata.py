from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPISKUMetadataCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_kpi_sku_metadata(
        self,
        target_year: int,
        target_month: int,
        interval_days: int = 30
    ) -> List[Dict]:
        if target_month == 1:
            recent_month = 12
            recent_year = target_year - 1
        else:
            recent_month = target_month - 1
            recent_year = target_year

        skus_in_recent_month = self.revenue_helper.get_skus_with_revenue_in_month(
            target_year=recent_year,
            target_month=recent_month
        )

        if not skus_in_recent_month:
            return []

        sku_filter_list = []
        for brand_name, sku in skus_in_recent_month:
            brand_name_escaped = brand_name.replace("'", "''")
            sku_escaped = sku.replace("'", "''")
            sku_filter_list.append(f"('{brand_name_escaped}', '{sku_escaped}')")
        sku_filter_str = ", ".join(sku_filter_list)

        query = f"""
            WITH
            rev_by_sku AS (
                SELECT 
                    brand_name,
                    CAST(sku AS UInt64) AS sku,
                    SUM(COALESCE(total_amount, 0)) AS revenue
                FROM hskcdp.object_sql_transaction_details FINAL
                WHERE toMonth(created_at) = {recent_month}
                  AND status NOT IN ('Canceled', 'Cancel')
                  AND (brand_name, CAST(sku AS String)) IN ({sku_filter_str})
                GROUP BY brand_name, sku
            ),
            total_rev_by_brand AS (
                SELECT 
                    brand_name, 
                    SUM(COALESCE(total_amount, 0)) AS total_revenue_by_brand 
                FROM hskcdp.object_sql_transaction_details FINAL
                WHERE toMonth(created_at) = {recent_month}
                  AND status NOT IN ('Canceled', 'Cancel')
                GROUP BY brand_name
            ),
            sku_with_share AS (
                SELECT
                    r.brand_name,
                    r.sku,
                    r.revenue,
                    t.total_revenue_by_brand,
                    r.revenue / t.total_revenue_by_brand * 100 AS revenue_distribution_by_sku,
                    SUM(r.revenue) OVER (
                        PARTITION BY r.brand_name
                        ORDER BY r.revenue DESC, r.sku
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) / t.total_revenue_by_brand * 100
                    AS cum_rev_share
                FROM rev_by_sku r
                INNER JOIN total_rev_by_brand t
                    ON r.brand_name = t.brand_name
            ),
            classified AS (
                SELECT
                    *,
                    CASE 
                        WHEN (cum_rev_share <= 80 OR revenue_distribution_by_sku >= 40) THEN 'Hero'
                        WHEN (cum_rev_share <= 95 OR revenue_distribution_by_sku >= 10) THEN 'Core'
                        ELSE 'Tail'
                    END AS sku_classification
                FROM sku_with_share
            ),
            final_calc AS (
                SELECT
                    *,
                    SUM(revenue) OVER (PARTITION BY brand_name, sku_classification) AS class_revenue,
                    revenue / SUM(revenue) OVER (PARTITION BY brand_name, sku_classification) * 100 AS revenue_share_in_class
                FROM classified
            )
            SELECT 
                brand_name,
                CAST(sku AS String) AS sku,
                revenue,
                total_revenue_by_brand,
                revenue_distribution_by_sku,
                cum_rev_share,
                sku_classification,
                class_revenue,
                revenue_share_in_class
            FROM final_calc
            ORDER BY brand_name, revenue DESC, sku
        """

        result = self.client.query(query)

        results: List[Dict] = []
        for row in result.result_rows:
            results.append({
                "year": target_year,
                "month": target_month,
                "brand_name": str(row[0]),
                "sku": str(row[1]),
                "revenue": Decimal(str(row[2])),
                "total_revenue_by_brand": Decimal(str(row[3])),
                "revenue_distribution_by_sku": Decimal(str(row[4])),
                "cum_rev_share": Decimal(str(row[5])),
                "sku_classification": str(row[6]),
                "class_revenue": Decimal(str(row[7])),
                "revenue_share_in_class": Decimal(str(row[8])),
            })

        return results
    
    def save_kpi_sku_metadata(self, metadata_data: List[Dict]) -> None:
        if not metadata_data:
            return
        
        now = datetime.now()
        
        data = []
        for row in metadata_data:
            data.append([
                row['year'],
                row['month'],
                row['brand_name'],
                row['sku'],
                float(row['revenue']),
                float(row['total_revenue_by_brand']),
                float(row['revenue_distribution_by_sku']),
                float(row['cum_rev_share']),
                row['sku_classification'],
                float(row['class_revenue']),
                float(row['revenue_share_in_class']),
                now,
                now
            ])
        
        columns = [
            'year', 'month', 'brand_name', 'sku', 'revenue', 'total_revenue_by_brand',
            'revenue_distribution_by_sku', 'cum_rev_share', 'sku_classification',
            'class_revenue', 'revenue_share_in_class',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_sku_metadata", data, column_names=columns)
    
    def calculate_and_save_kpi_sku_metadata(
        self,
        target_year: int,
        target_month: int,
        interval_days: int = 30
    ) -> List[Dict]:
        metadata_data = self.calculate_kpi_sku_metadata(
            target_year=target_year,
            target_month=target_month,
            interval_days=interval_days
        )
        
        self.save_kpi_sku_metadata(metadata_data)
        
        return metadata_data


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPISKUMetadataCalculator(constants)
    
    target_month = None
    target_year = constants.KPI_YEAR_2026
    interval_days = 30
    
    if len(sys.argv) > 1:
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--target-month" and i + 1 < len(sys.argv):
                target_month = int(sys.argv[i + 1])
                i += 2
            elif sys.argv[i] == "--target-year" and i + 1 < len(sys.argv):
                target_year = int(sys.argv[i + 1])
                i += 2
            elif sys.argv[i] == "--interval-days" and i + 1 < len(sys.argv):
                interval_days = int(sys.argv[i + 1])
                i += 2
            else:
                i += 1
    
    if target_month is None:
        today = date.today()
        target_month = today.month + 1
        if target_month > 12:
            target_month = 1
            target_year = today.year + 1
        else:
            target_year = today.year
    
    if target_month < 1 or target_month > 12:
        print(f"Error: target_month must be between 1 and 12, received: {target_month}")
        sys.exit(1)
    
    print(f"Calculating kpi_sku_metadata for month {target_month}/{target_year}...")
    metadata_data = calculator.calculate_and_save_kpi_sku_metadata(
        target_year=target_year,
        target_month=target_month,
        interval_days=interval_days
    )
    
    print(f"Successfully saved {len(metadata_data)} kpi_sku_metadata records")
