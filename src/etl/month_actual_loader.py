from datetime import datetime
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class ActualMonthLoader:   
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def insert_actual(self, month: int, actual_amount: float) -> None:
        now = datetime.now()
        
        check_query = f"""
            SELECT created_at
            FROM hskcdp.actual_2026_staging FINAL
            WHERE year = {self.constants.KPI_YEAR_2026} 
            AND month = {month}
            LIMIT 1
        """
        result = self.client.query(check_query)
        created_at = result.result_rows[0][0] if result.result_rows else now
        
        data = [[
            self.constants.KPI_YEAR_2026,
            month,
            actual_amount,
            False,  
            None,  
            created_at,  
            now     
        ]]
        
        columns = ['year', 'month', 'actual_amount', 'processed', 'processed_at', 'created_at', 'updated_at']
        self.client.insert("hskcdp.actual_2026_staging", data, column_names=columns)
    
    def mark_processed(self, month: Optional[int] = None) -> None:
        now = datetime.now()
        
        if month:
            get_query = f"""
                SELECT month, actual_amount, created_at
                FROM hskcdp.actual_2026_staging FINAL
                WHERE year = {self.constants.KPI_YEAR_2026} 
                AND month = {month}
                AND processed = false
                LIMIT 1
            """
        else:
            get_query = f"""
                SELECT month, actual_amount, created_at
                FROM hskcdp.actual_2026_staging FINAL
                WHERE year = {self.constants.KPI_YEAR_2026} 
                AND processed = false
            """
        
        result = self.client.query(get_query)
        rows = result.result_rows
        
        if rows:
            data = []
            for row in rows:
                data.append([
                    self.constants.KPI_YEAR_2026,
                    row[0],  # month
                    float(row[1]),  # actual_amount - ensure float
                    True,    
                    now,     
                    row[2],  
                    now      
                ])
            
            columns = ['year', 'month', 'actual_amount', 'processed', 'processed_at', 'created_at', 'updated_at']
            self.client.insert("hskcdp.actual_2026_staging", data, column_names=columns)
    
    def get_unprocessed_actuals(self) -> List[Dict]:
        query = f"""
            SELECT 
                year,
                month,
                actual_amount,
                processed,
                processed_at,
                created_at,
                updated_at
            FROM hskcdp.actual_2026_staging FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
            AND processed = false
            ORDER BY month
        """
        
        result = self.client.query(query)
        rows = result.result_rows
        
        return [
            {
                'year': row[0],
                'month': row[1],
                'actual_amount': float(row[2]),  # ensure float
                'processed': row[3],
                'processed_at': row[4],
                'created_at': row[5],
                'updated_at': row[6]
            }
            for row in rows
        ]
    
    def get_processed_actuals(self) -> List[Dict]:
        query = f"""
            SELECT 
                year,
                month,
                actual_amount,
                processed,
                processed_at,
                created_at,
                updated_at
            FROM hskcdp.actual_2026_staging FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
            AND processed = true
            ORDER BY month
        """
        
        result = self.client.query(query)
        rows = result.result_rows
        
        return [
            {
                'year': row[0],
                'month': row[1],
                'actual_amount': float(row[2]),  # ensure float
                'processed': row[3],
                'processed_at': row[4],
                'created_at': row[5],
                'updated_at': row[6]
            }
            for row in rows
        ]


if __name__ == "__main__":
    constants = Constants()
    loader = ActualMonthLoader(constants)
    
    print("Inserting actual for month 1...")
    loader.insert_actual(month=2, actual_amount=700425368454)
    
    print("Marking as processed...")
    loader.mark_processed(month=2)
    
    print("Getting processed actuals...")
    processed = loader.get_processed_actuals()
    print(f"Found {len(processed)} processed actuals")