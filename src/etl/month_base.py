from decimal import Decimal
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants

class BaseKPICalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def calculate_base_kpi(self) -> List[Dict]:
        query = f"""
            SELECT 
                month,
                total
            FROM hskcdp.revenue_2025
            WHERE year = {self.constants.BASE_YEAR}
            ORDER BY month
        """
        
        result = self.client.query(query)
        revenue_2025 = result.result_rows  # Returns list of tuples
        
        total_2025 = sum(float(row[1]) for row in revenue_2025)
        
        if total_2025 == 0:
            raise ValueError("Total revenue 2025 cannot be 0")
        
        results = []
        for month, month_revenue in revenue_2025:
            month_revenue_float = float(month_revenue)
            distribution = (month_revenue_float / total_2025) * 100
            
            kpi_initial = float(self.constants.KPI_TOTAL_2026) * (distribution / 100)
            
            results.append({
                'year': self.constants.KPI_YEAR_2026,
                'month': month,
                'kpi_initial': float(kpi_initial),
                'distribution_revenue': float(distribution),
                'total_2025': float(month_revenue_float)
            })
        
        return results
    
    def save_base_kpi(self):
        base_kpi = self.calculate_base_kpi()
  
        self.client.command("TRUNCATE TABLE hskcdp.kpi_month_base")
        
        data = [
            [
                row['year'],
                row['month'],
                row['kpi_initial'],
                row['distribution_revenue'],
                row['total_2025']
            ]
            for row in base_kpi
        ]
        
        columns = ['year', 'month', 'kpi_initial', 'distribution_revenue', 'total_2025']
        self.client.insert("hskcdp.kpi_month_base", data, column_names=columns)
        
        return base_kpi


if __name__ == "__main__":
    constants = Constants()
    
    calculator = BaseKPICalculator(constants)
    
    print("Calculating base KPI...")
    result = calculator.save_base_kpi()
    
    print(f"Successfully saved {len(result)} records to kpi_month_base")