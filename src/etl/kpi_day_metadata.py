from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIDayMetadataCalculator:    
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_uplift_from_historical(
        self, 
        target_year: int,
        target_month: int,
        date_labels: List[str] = None
    ) -> Dict[str, Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
        historical_data = self.revenue_helper.get_historical_revenue_by_date_label(
            date_labels
        )
        
        baseline = historical_data.get('Normal day', {}).get('avg_total', 0)
        
        if baseline == 0:
            raise ValueError("Cannot calculate uplift: baseline (Normal day) avg_total is 0")
        
        uplifts = {}
        for date_label in date_labels:
            if date_label in historical_data:
                avg_total = historical_data[date_label]['avg_total']
                uplift = avg_total / baseline
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
        date_labels: List[str] = None
    ) -> List[Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
            
        uplifts = self.calculate_uplift_from_historical(
            target_year,
            target_month,
            date_labels
        )
        
        weights = self.calculate_weight_for_month(
            target_year,
            target_month,
            date_labels
        )
        
        # Calculate historical_start_date and historical_end_date only save into metadata table
        today = date.today()
        historical_end_date = today
        historical_start_date = today - timedelta(days=90)
        
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
                'historical_start_date': historical_start_date,
                'historical_end_date': historical_end_date
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
    
    def check_metadata_annually_exists(
        self,
        target_year: int,
        target_month: int
    ) -> bool:
        """Check xem có data trong metadata_annually cho tháng đang chạy không"""
        query = f"""
            SELECT COUNT(*) 
            FROM hskcdp.metadata_annually
            WHERE year = {target_year}
              AND month = {target_month}
        """
        
        result = self.client.query(query)
        count = result.result_rows[0][0] if result.result_rows else 0
        
        return count > 0
    
    def get_metadata_annually_data(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        """Lấy data từ metadata_annually cho tháng đang chạy"""
        query = f"""
            SELECT year, month, priority_label, uplift
            FROM hskcdp.metadata_annually
            WHERE year = {target_year}
              AND month = {target_month}
        """
        
        result = self.client.query(query)
        data = []
        
        for row in result.result_rows:
            data.append({
                'year': int(row[0]),
                'month': int(row[1]),
                'priority_label': str(row[2]),
                'uplift': float(row[3])
            })
        
        return data
    
    def save_metadata_from_annually(
        self,
        target_year: int,
        target_month: int
    ) -> None:
        """Insert data từ metadata_annually vào kpi_day_metadata"""
        annually_data = self.get_metadata_annually_data(target_year, target_month)
        
        if not annually_data:
            return
        
        # Calculate historical_start_date and historical_end_date (giữ logic hiện tại)
        today = date.today()
        historical_end_date = today
        historical_start_date = today - timedelta(days=90)
        
        now = datetime.now()
        
        data = []
        for row in annually_data:
            priority_label = row['priority_label']
            uplift = row['uplift']
            so_ngay = 1  # Cố định = 1 cho ngày lễ annually
            weight = uplift  # weight = so_ngay * uplift = 1 * uplift = uplift
            
            data.append([
                target_year,
                target_month,
                priority_label,  # date_label = priority_label
                0.0,  # avg_total = 0
                float(uplift),
                so_ngay,
                float(weight),
                0.0,  # total_weight_month = 0 (tạm thời, sẽ update sau)
                historical_start_date,
                historical_end_date,
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
    
    def update_total_weight_month(
        self,
        target_year: int,
        target_month: int
    ) -> float:
        """Tính tổng weight và update total_weight_month cho tháng"""
        # Bước 1: Tính tổng weight
        query = f"""
            SELECT SUM(weight)
            FROM hskcdp.kpi_day_metadata FINAL
            WHERE year = {target_year}
              AND month = {target_month}
        """
        
        result = self.client.query(query)
        sum_weight = result.result_rows[0][0] if result.result_rows else 0
        
        # Bước 2: Update total_weight_month
        update_query = f"""
            ALTER TABLE hskcdp.kpi_day_metadata
            UPDATE total_weight_month = {sum_weight}
            WHERE year = {target_year}
              AND month = {target_month}
        """
        
        self.client.command(update_query)
        
        return float(sum_weight) if sum_weight else 0.0
    
    def calculate_and_save_metadata(
        self,
        target_year: int,
        target_month: int,
        date_labels: List[str] = None
    ) -> List[Dict]:
        # Luôn chạy logic hiện tại trước
        metadata = self.calculate_metadata(
            target_year=target_year,
            target_month=target_month,
            date_labels=date_labels
        )
        
        self.save_metadata(metadata)
        
        # Nếu có data trong metadata_annually, thêm các step bổ sung
        if self.check_metadata_annually_exists(target_year, target_month):
            # Insert thêm data từ metadata_annually
            self.save_metadata_from_annually(target_year, target_month)
            # Recalculate và update total_weight_month cho cả tháng
            self.update_total_weight_month(target_year, target_month)
        
        return metadata


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPIDayMetadataCalculator(constants)
    
    target_month = None
    target_year = constants.KPI_YEAR_2026
    
    if len(sys.argv) > 1:
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--target-month" and i + 1 < len(sys.argv):
                target_month = int(sys.argv[i + 1])
                i += 2
            elif sys.argv[i] == "--target-year" and i + 1 < len(sys.argv):
                target_year = int(sys.argv[i + 1])
                i += 2
            else:
                i += 1
    
    if target_month is None:
        today = date.today()
        if today.year == constants.KPI_YEAR_2026:
            target_month = today.month + 1
            if target_month > 12:
                target_month = 1
                target_year = constants.KPI_YEAR_2026 + 1
        else:
            target_month = 1
    
    if target_month < 1 or target_month > 12:
        print(f"Error: target_month must be between 1 and 12, received: {target_month}")
        sys.exit(1)
    
    print(f"Calculating metadata for month {target_month}/{target_year}...")
    metadata = calculator.calculate_and_save_metadata(
        target_year=target_year,
        target_month=target_month
    )
    
    print(f"Successfully saved {len(metadata)} metadata records")