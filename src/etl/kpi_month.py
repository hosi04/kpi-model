from decimal import Decimal
from datetime import datetime
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class KPIAdjustmentCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def calculate_kpi_adjustment(self) -> List[Dict]:
        query_check = f"""
            SELECT 
                distinct version
            FROM hskcdp.kpi_month
            WHERE year = {self.constants.KPI_YEAR_2026}
        """
        result_check = self.client.query(query_check)
        versions = [row[0] for row in result_check.result_rows]
        if len(versions) == 0:
            base_query = f"""
                SELECT 
                    year,
                    month,
                    kpi_initial
                FROM hskcdp.kpi_month_base
                WHERE year = {self.constants.KPI_YEAR_2026}
                ORDER BY month
            """
        else:
            base_query = f"""
                SELECT 
                    year,
                    month,
                    kpi_adjustment
                FROM hskcdp.kpi_month
                WHERE year = {self.constants.KPI_YEAR_2026}
                ORDER BY month
            """
        
        base_result = self.client.query(base_query)
        base_kpi = {row[1]: {'year': row[0], 'month': row[1], 'kpi_initial': float(row[2])} 
                   for row in base_result.result_rows}
        
        actual_query = f"""
            SELECT 
                month,
                actual_amount
            FROM hskcdp.actual_2026_staging FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
            AND processed = true
            ORDER BY month
        """
        actual_result = self.client.query(actual_query)
        actuals = {row[0]: float(row[1]) for row in actual_result.result_rows}
        
        latest_month_with_actual = max(actuals.keys()) if actuals else 0
        version = f"Thang {latest_month_with_actual}" if latest_month_with_actual > 0 else "Thang 0"
        
        gaps = {}
        total_gap = Decimal('0')
        
        for month in actuals.keys():
            kpi_initial = Decimal(str(base_kpi[month]['kpi_initial']))
            actual = Decimal(str(actuals[month]))
            gap = actual - kpi_initial
            gaps[month] = gap
            total_gap += gap
        
        months_with_actual = len(actuals)
        remaining_months = 12 - months_with_actual
        
        if remaining_months > 0:
            gap_per_remaining_month = total_gap / Decimal(str(remaining_months))
        else:
            gap_per_remaining_month = Decimal('0')
        
        results = []
        for month in range(1, 13):
            kpi_initial = Decimal(str(base_kpi[month]['kpi_initial']))
            
            if month in actuals:
                actual_2026 = Decimal(str(actuals[month]))
                gap = gaps[month]
                kpi_adjustment = actual_2026
            else:
                actual_2026 = None
                gap = None
                kpi_adjustment = kpi_initial - gap_per_remaining_month
            
            results.append({
                'version': version,
                'year': self.constants.KPI_YEAR_2026,
                'month': month,
                'kpi_initial': float(kpi_initial),
                'actual_2026': float(actual_2026) if actual_2026 is not None else None,
                'gap': float(gap) if gap is not None else None,
                'kpi_adjustment': float(kpi_adjustment)
            })
        
        return results
    
    def save_kpi_adjustment(self) -> List[Dict]:
        results = self.calculate_kpi_adjustment()
        
        now = datetime.now()
        
        data = []
        for row in results:
            data.append([
                row['version'],
                row['year'],
                row['month'],
                row['kpi_initial'],
                row['actual_2026'],
                row['gap'],
                row['kpi_adjustment'],
                now,  
                now   
            ])
        
        columns = [
            'version', 'year', 'month', 'kpi_initial', 'actual_2026', 'gap',
            'kpi_adjustment', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_month", data, column_names=columns)
        
        return results


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIAdjustmentCalculator(constants)
    
    print("Calculating KPI adjustment...")
    result = calculator.save_kpi_adjustment()
    
    print(f"Successfully saved {len(result)} records to kpi_month")
