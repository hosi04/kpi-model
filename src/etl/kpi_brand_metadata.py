from decimal import Decimal
from datetime import datetime
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIBrandMetadataCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_kpi_brand_metadata(
        self,
        target_month: int
    ) -> List[Dict]:
        # Sử dụng helper method để query từ object_sql_transaction_details (3 tháng gần nhất)
        revenue_by_brand = self.revenue_helper.get_revenue_by_brand_last_3_months()
        
        # Lọc chỉ lấy brands có revenue > 0
        positive_revenue_brands = {
            brand_name: revenue 
            for brand_name, revenue in revenue_by_brand.items() 
            if revenue > 0
        }
        
        # Tính tổng revenue của các brands có revenue > 0
        total_revenue = sum(positive_revenue_brands.values())
        
        if total_revenue == 0:
            raise ValueError("Cannot calculate brand metadata: total revenue is 0")
        
        results = []
        for brand_name, brand_revenue in sorted(positive_revenue_brands.items()):
            
            # Tính per_of_rev_by_brand = brand_revenue / total_revenue
            per_of_rev_by_brand = brand_revenue / total_revenue
            
            # Initially, per_of_rev_by_brand_adj equals per_of_rev_by_brand
            # pic is empty string for now
            per_of_rev_by_brand_adj = per_of_rev_by_brand
            
            results.append({
                'month': target_month,
                'brand_name': brand_name,
                'per_of_rev_by_brand': float(per_of_rev_by_brand),
                'pic': '',  # Empty string for now
                'per_of_rev_by_brand_adj': float(per_of_rev_by_brand_adj)
            })
        
        return results
    
    def save_kpi_brand_metadata(self, metadata_data: List[Dict]) -> None:
        if not metadata_data:
            return
        
        now = datetime.now()
        
        data = []
        for row in metadata_data:
            data.append([
                row['month'],
                row['brand_name'],
                row['per_of_rev_by_brand'],
                row['pic'],
                row['per_of_rev_by_brand_adj'],
                now,
                now
            ])
        
        columns = [
            'month', 'brand_name', 'per_of_rev_by_brand', 'pic', 'per_of_rev_by_brand_adj',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_brand_metadata", data, column_names=columns)
    
    def calculate_and_save_kpi_brand_metadata(
        self,
        target_month: int
    ) -> List[Dict]:
        metadata_data = self.calculate_kpi_brand_metadata(
            target_month=target_month
        )
        
        self.save_kpi_brand_metadata(metadata_data)
        
        return metadata_data


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIBrandMetadataCalculator(constants)
    
    print("Calculating kpi_brand_metadata for month 1...")
    metadata_data = calculator.calculate_and_save_kpi_brand_metadata(
        target_month=1
    )
    
    print(f"Successfully saved {len(metadata_data)} kpi_brand_metadata records")

