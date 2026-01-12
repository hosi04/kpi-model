from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class KPIDayMetadataCalculator:    
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def calculate_uplift_from_historical(
        self, 
        historical_months: List[int],
        historical_year: int,
        date_labels: List[str] = None
    ) -> Dict[str, Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
        # Add Normal day to the list if it's not already there
        if 'Normal day' not in date_labels:
            date_labels = ['Normal day'] + date_labels
        
        historical_start_date = date(historical_year, min(historical_months), 1)
        
        if max(historical_months) == 12:
            historical_end_date = date(historical_year, 12, 31)
        else:
            from datetime import timedelta
            if max(historical_months) + 1 <= 12:
                next_month = date(historical_year, max(historical_months) + 1, 1)
            else:
                next_month = date(historical_year + 1, 1, 1)
            historical_end_date = next_month - timedelta(days=1)
        # historical_start_date = 01/10/2025
        # historical_end_date = 31/12/2025
        
        query = f"""
            SELECT 
                date_label,
                AVG(daily_revenue) as avg_total,
                COUNT(*) as so_ngay_historical
            FROM (
                SELECT 
                    calendar_date,
                    date_label,
                    SUM(revenue) as daily_revenue
                FROM hskcdp.revenue_2025_ver2
                WHERE calendar_date >= '{historical_start_date}'
                  AND calendar_date <= '{historical_end_date}'
                  AND date_label IN ({','.join([f"'{dl}'" for dl in date_labels])})
                  AND NOT (
                      (toMonth(calendar_date) = 6 AND toDayOfMonth(calendar_date) = 6) OR
                      (toMonth(calendar_date) = 9 AND toDayOfMonth(calendar_date) = 9) OR
                      (toMonth(calendar_date) = 11 AND toDayOfMonth(calendar_date) = 11) OR
                      (toMonth(calendar_date) = 12 AND toDayOfMonth(calendar_date) = 12)
                  )
                GROUP BY calendar_date, date_label
            )
            GROUP BY date_label
        """
        
        result = self.client.query(query)
        historical_data = {}
        
        for row in result.result_rows:
            date_label = row[0]
            avg_total = float(row[1])
            so_ngay_historical = int(row[2])
            historical_data[date_label] = {
                'avg_total': avg_total,
                'so_ngay_historical': so_ngay_historical
            }
        
        baseline = historical_data.get('Normal day', {}).get('avg_total', 0)
        
        if baseline == 0:
            raise ValueError("Cannot calculate uplift: baseline (Normal day) avg_total is 0")
        
        uplifts = {}
        for date_label in date_labels:
            if date_label in historical_data:
                avg_total = historical_data[date_label]['avg_total']
                uplift = avg_total / baseline if baseline > 0 else 0
            else:
                avg_total = 0
                uplift = 0
            
            uplifts[date_label] = {
                'avg_total': avg_total,
                'uplift': uplift
            }
        
        return uplifts
    
    def calculate_weight_for_month(
        self,
        target_year: int,
        target_month: int,
        date_labels: List[str] = None
    ) -> Dict[str, Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
        if 'Normal day' not in date_labels:
            date_labels = ['Normal day'] + date_labels
        
        query = f"""
            SELECT 
                date_label,
                COUNT(*) as so_ngay
            FROM dim_date
            WHERE year = {target_year}
              AND month = {target_month}
              AND date_label IN ({','.join([f"'{dl}'" for dl in date_labels])})
              AND NOT (
                  (month = 6 AND day = 6) OR
                  (month = 9 AND day = 9) OR
                  (month = 11 AND day = 11) OR
                  (month = 12 AND day = 12)
              )
            GROUP BY date_label
        """
        
        result = self.client.query(query)
        weights = {}
        
        for row in result.result_rows:
            date_label = row[0]
            so_ngay = int(row[1])
            weights[date_label] = {
                'so_ngay': so_ngay
            }
        
        return weights
    
    def calculate_metadata(
        self,
        target_year: int,
        target_month: int,
        historical_months: List[int],
        historical_year: int,
        version: str,
        date_labels: List[str] = None
    ) -> List[Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
            
        if 'Normal day' not in date_labels:
            date_labels = ['Normal day'] + date_labels
        
        uplifts = self.calculate_uplift_from_historical(
            historical_months,
            historical_year,
            date_labels
        )
        
        weights = self.calculate_weight_for_month(
            target_year,
            target_month,
            date_labels
        )
        
        results = []
        total_weight = Decimal('0')
        
        for date_label in date_labels:
            uplift_data = uplifts.get(date_label, {})
            weight_data = weights.get(date_label, {})
            
            if not uplift_data or not weight_data:
                continue
            
            avg_total = uplift_data.get('avg_total', 0)
            uplift = uplift_data.get('uplift', 0)
            so_ngay = weight_data.get('so_ngay', 0)
            weight = Decimal(str(so_ngay)) * Decimal(str(uplift))
            total_weight += weight
            
            results.append({
                'year': target_year,
                'month': target_month,
                'date_label': date_label,
                'avg_total': float(avg_total),
                'uplift': float(uplift),
                'so_ngay': so_ngay,
                'weight': float(weight),
                'total_weight_month': 0,
                'historical_start_date': date(historical_year, min(historical_months), 1),
                'historical_end_date': date(historical_year, max(historical_months), 28)
            })
        
        for result in results:
            result['total_weight_month'] = float(total_weight)
        
        return results
    
    def save_metadata(self, metadata: List[Dict]) -> None:
        if not metadata:
            return
        
        now = datetime.now()
        
        data = []
        for row in metadata:
            data.append([
                row['year'],
                row['month'],
                row['date_label'],
                row['avg_total'],
                row['uplift'],
                row['so_ngay'],
                row['weight'],
                row['total_weight_month'],
                row['historical_start_date'],
                row['historical_end_date'],
                now,
                now
            ])
        
        columns = [
            'year', 'month', 'date_label', 'avg_total', 'uplift',
            'so_ngay', 'weight', 'total_weight_month',
            'historical_start_date', 'historical_end_date',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_day_metadata", data, column_names=columns)
    
    def calculate_and_save_metadata(
        self,
        target_year: int,
        target_month: int,
        historical_months: List[int],
        historical_year: int,
        version: Optional[str] = None,
        date_labels: List[str] = None
    ) -> List[Dict]:
        if version is None:
            version = f"Thang {target_month}"
        
        historical_start_date = date(historical_year, min(historical_months), 1)
        
        if max(historical_months) == 12:
            historical_end_date = date(historical_year, 12, 31)
        else:
            if max(historical_months) + 1 <= 12:
                next_month = date(historical_year, max(historical_months) + 1, 1)
            else:
                next_month = date(historical_year + 1, 1, 1)
            from datetime import timedelta
            historical_end_date = next_month - timedelta(days=1)
        
        metadata = self.calculate_metadata(
            target_year=target_year,
            target_month=target_month,
            historical_months=historical_months,
            historical_year=historical_year,
            version=version,
            date_labels=date_labels
        )
        
        self.save_metadata(metadata)
        
        return metadata


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIDayMetadataCalculator(constants)
    
    print("Calculating metadata for month 1/2026...")
    metadata = calculator.calculate_and_save_metadata(
        target_year=2026,
        target_month=1,
        historical_months=[10, 11, 12],
        historical_year=2025
    )
    
    print(f"Successfully saved {len(metadata)} metadata records")