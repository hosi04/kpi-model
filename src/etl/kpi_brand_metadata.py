from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIBrandMetadataCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_and_save_kpi_brand_metadata(
        self,
        target_year: int,
        target_month: int,
        interval_days: int = 90
    ) -> List[Dict]:
        # 1. Fetch data
        revenue_by_brand = self.revenue_helper.get_revenue_by_brand_last_n_days(interval_days=interval_days) 

        # Xác định tháng gần nhất để filter brand có phát sinh doanh thu
        if target_month == 1:
            recent_month, recent_year = 12, target_year - 1
        else:
            recent_month, recent_year = target_month - 1, target_year
        
        brands_in_recent_month = self.revenue_helper.get_brands_with_revenue_in_month(
            target_year=recent_year,
            target_month=recent_month
        )

        # Filter brands có doanh thu dương và có bán trong tháng gần nhất
        positive_revenue_brands = {
            brand_name: revenue 
            for brand_name, revenue in revenue_by_brand.items() 
            if revenue > 0 and brand_name in brands_in_recent_month
        }
        
        total_revenue = sum(positive_revenue_brands.values())
        if total_revenue == 0:
            raise ValueError("Cannot calculate brand metadata: total revenue is 0")
        
        # 2. Calculate and Prepare for Save
        results = []
        data_to_insert = []
        now = datetime.now()
        sum_check = 0.0   

        for brand_name, brand_revenue in sorted(positive_revenue_brands.items()):
            per_of_rev_by_brand = brand_revenue / total_revenue
            sum_check += per_of_rev_by_brand
            per_of_rev_by_brand_adj = per_of_rev_by_brand
            
            # Record for return
            results.append({
                'year': target_year,
                'month': target_month,
                'brand_name': brand_name,
                'per_of_rev_by_brand': float(per_of_rev_by_brand),
                'pic': '',
                'per_of_rev_by_brand_adj': float(per_of_rev_by_brand_adj)
            })
            
            # Row for database insert
            data_to_insert.append([
                target_year,
                target_month,
                brand_name,
                float(per_of_rev_by_brand),
                '', # pic
                float(per_of_rev_by_brand_adj),
                now,
                now
            ])

        print(f"=========sum_check ({target_month}/{target_year})========= ", sum_check)
        
        # 3. Save to DB
        if data_to_insert:
            columns = [
                'year', 'month', 'brand_name', 'per_of_rev_by_brand', 'pic', 'per_of_rev_by_brand_adj',
                'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_brand_metadata", data_to_insert, column_names=columns)
        
        return results


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPIBrandMetadataCalculator(constants)
    
    target_month = None
    target_year = constants.KPI_YEAR_2026
    interval_days = 90
    
    if len(sys.argv) > 1:
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--target-month" and i + 1 < len(sys.argv):
                target_month = int(sys.argv[i + 1]); i += 2
            elif sys.argv[i] == "--target-year" and i + 1 < len(sys.argv):
                target_year = int(sys.argv[i + 1]); i += 2
            elif sys.argv[i] == "--interval-days" and i + 1 < len(sys.argv):
                interval_days = int(sys.argv[i + 1]); i += 2
            else: i += 1
    
    if target_month is None:
        today = date.today()
        target_month = today.month + 1
        if target_month > 12:
            target_month = 1; target_year = today.year + 1
        else: target_year = today.year
    
    print(f"Calculating kpi_brand_metadata for month {target_month}/{target_year}...")
    metadata_data = calculator.calculate_and_save_kpi_brand_metadata(
        target_year=target_year,
        target_month=target_month,
        interval_days=interval_days
    )
    
    print(f"Successfully saved {len(metadata_data)} kpi_brand_metadata records")
