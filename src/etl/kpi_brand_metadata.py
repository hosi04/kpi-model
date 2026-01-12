from decimal import Decimal
from datetime import datetime
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class KPIBrandMetadataCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def calculate_kpi_brand_metadata(
        self,
        target_month: int
    ) -> List[Dict]:
        # Calculate revenue percentage by brand from revenue_2025_ver3
        query = f"""
            SELECT 
                brand_name,
                SUM(revenue) / revenue_1.total AS per_of_rev_by_brand
            FROM hskcdp.revenue_2025_ver3
            CROSS JOIN (SELECT SUM(revenue) AS total FROM hskcdp.revenue_2025_ver3) AS revenue_1
            GROUP BY brand_name, total
            ORDER BY brand_name
        """
        
        result = self.client.query(query)
        
        results = []
        for row in result.result_rows:
            brand_name = str(row[0])
            per_of_rev_by_brand = float(row[1])
            
            # Initially, per_of_rev_by_brand_adj equals per_of_rev_by_brand
            # pic is empty string for now
            per_of_rev_by_brand_adj = per_of_rev_by_brand
            
            results.append({
                'month': target_month,
                'brand_name': brand_name,
                'per_of_rev_by_brand': per_of_rev_by_brand,
                'pic': '',  # Empty string for now
                'per_of_rev_by_brand_adj': per_of_rev_by_brand_adj
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

