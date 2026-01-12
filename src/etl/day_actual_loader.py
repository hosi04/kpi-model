from datetime import datetime, date
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class ActualDayLoader:   
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def insert_actual(self, calendar_date: date, actual_amount: float) -> None:
        now = datetime.now()
        
        check_query = f"""
            SELECT created_at
            FROM hskcdp.actual_2026_day_staging FINAL
            WHERE year = {self.constants.KPI_YEAR_2026} 
            AND calendar_date = '{calendar_date}'
            LIMIT 1
        """
        result = self.client.query(check_query)
        created_at = result.result_rows[0][0] if result.result_rows else now
        
        month = calendar_date.month
        day = calendar_date.day
        
        data = [[
            self.constants.KPI_YEAR_2026,
            calendar_date,
            month,
            day,
            actual_amount,
            False,  
            None,  
            created_at,  
            now     
        ]]
        
        columns = ['year', 'calendar_date', 'month', 'day', 'actual_amount', 'processed', 'processed_at', 'created_at', 'updated_at']
        self.client.insert("hskcdp.actual_2026_day_staging", data, column_names=columns)
    
    def mark_processed(self, calendar_date: Optional[date] = None) -> None:
        now = datetime.now()
        
        if calendar_date:
            get_query = f"""
                SELECT calendar_date, year, month, day, actual_amount, created_at
                FROM hskcdp.actual_2026_day_staging FINAL
                WHERE year = {self.constants.KPI_YEAR_2026} 
                AND calendar_date = '{calendar_date}'
                AND processed = false
                LIMIT 1
            """
        else:
            get_query = f"""
                SELECT calendar_date, year, month, day, actual_amount, created_at
                FROM hskcdp.actual_2026_day_staging FINAL
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
                    row[0],  # calendar_date
                    row[2],  # month
                    row[3],  # day
                    float(row[4]),  # actual_amount - ensure float
                    True,    
                    now,     
                    row[5],  # created_at
                    now      
                ])
            
            columns = ['year', 'calendar_date', 'month', 'day', 'actual_amount', 'processed', 'processed_at', 'created_at', 'updated_at']
            self.client.insert("hskcdp.actual_2026_day_staging", data, column_names=columns)
    
    def get_unprocessed_actuals(self) -> List[Dict]:
        query = f"""
            SELECT 
                year,
                calendar_date,
                month,
                day,
                actual_amount,
                processed,
                processed_at,
                created_at,
                updated_at
            FROM hskcdp.actual_2026_day_staging FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
            AND processed = false
            ORDER BY calendar_date
        """
        
        result = self.client.query(query)
        rows = result.result_rows
        
        return [
            {
                'year': row[0],
                'calendar_date': row[1],
                'month': row[2],
                'day': row[3],
                'actual_amount': float(row[4]),  # ensure float
                'processed': row[5],
                'processed_at': row[6],
                'created_at': row[7],
                'updated_at': row[8]
            }
            for row in rows
        ]
    
    def get_processed_actuals(self) -> List[Dict]:
        query = f"""
            SELECT 
                year,
                calendar_date,
                month,
                day,
                actual_amount,
                processed,
                processed_at,
                created_at,
                updated_at
            FROM hskcdp.actual_2026_day_staging FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
            AND processed = true
            ORDER BY calendar_date
        """
        
        result = self.client.query(query)
        rows = result.result_rows
        
        return [
            {
                'year': row[0],
                'calendar_date': row[1],
                'month': row[2],
                'day': row[3],
                'actual_amount': float(row[4]),  # ensure float
                'processed': row[5],
                'processed_at': row[6],
                'created_at': row[7],
                'updated_at': row[8]
            }
            for row in rows
        ]


if __name__ == "__main__":
    constants = Constants()
    loader = ActualDayLoader(constants)
    
    print("Inserting actual for day 2026-01-15...")
    loader.insert_actual(calendar_date=date(2026, 1, 2), actual_amount=18134947811)
    
    print("Marking as processed...")
    loader.mark_processed(calendar_date=date(2026, 1, 2))
    
    print("Getting processed actuals...")
    processed = loader.get_processed_actuals()
    print(f"Found {len(processed)} processed actuals")

