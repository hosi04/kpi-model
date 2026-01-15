from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIDayMetadataCalculator:    
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def _calculate_historical_months(self, target_year: int, target_month: int) -> Tuple[List[int], int]:
        """
        Tính 3 tháng gần nhất trước target_month
        Returns: (historical_months, historical_year)
        Ví dụ:
        - target_month=1/2026 → ([10, 11, 12], 2025)
        - target_month=2/2026 → ([11, 12], 2025) và ([1], 2026) → ([11, 12, 1], 2025) nhưng cần xử lý cross-year
        """
        historical_months = []
        historical_year = target_year
        historical_months_with_year = []  # Để debug
        
        # Tính 3 tháng trước target_month
        for i in range(3, 0, -1):  # 3, 2, 1
            month = target_month - i
            year = target_year
            
            if month <= 0:
                month += 12
                year -= 1
            
            historical_months.append(month)
            historical_months_with_year.append((month, year))
            # historical_year sẽ là năm của tháng đầu tiên trong list
            if i == 3:
                historical_year = year
        
        # Print debug
        print(f"[DEBUG] Target: {target_month}/{target_year}")
        print(f"[DEBUG] 3 tháng gần nhất: {[f'{m}/{y}' for m, y in historical_months_with_year]}")
        print(f"[DEBUG] historical_months: {historical_months}, historical_year: {historical_year}")
        
        return historical_months, historical_year
    
    def calculate_uplift_from_historical(
        self, 
        target_year: int,
        target_month: int,
        date_labels: List[str] = None
    ) -> Dict[str, Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
        # Add Normal day to the list if it's not already there
        if 'Normal day' not in date_labels:
            date_labels = ['Normal day'] + date_labels
        
        # Tự động tính 3 tháng gần nhất trước target_month
        historical_months, historical_year = self._calculate_historical_months(target_year, target_month)
        
        # Tính historical_start_date và historical_end_date
        # historical_months có thể cross-year (ví dụ: [11, 12, 1])
        # Cần tính start_date từ tháng đầu tiên, end_date từ tháng cuối cùng
        first_month = historical_months[0]
        last_month = historical_months[-1]
        
        # Xác định năm cho tháng đầu tiên và tháng cuối cùng
        first_month_year = historical_year
        
        # Nếu last_month < first_month thì đã cross-year (ví dụ: [11, 12, 1])
        # last_month sẽ ở năm target_year
        if last_month < first_month:
            last_month_year = target_year
        else:
            last_month_year = historical_year
        
        historical_start_date = date(first_month_year, first_month, 1)
        
        # Tính end_date của tháng cuối cùng
        if last_month == 12:
            historical_end_date = date(last_month_year, 12, 31)
        else:
            next_month = date(last_month_year, last_month + 1, 1)
            historical_end_date = next_month - timedelta(days=1)
        
        # Sử dụng helper method để query từ transactions
        # Truyền historical_months và historical_year để dùng toStartOfMonth filter
        historical_data = self.revenue_helper.get_historical_revenue_by_date_label(
            historical_start_date,
            historical_end_date,
            date_labels,
            historical_months=historical_months,
            historical_year=historical_year
        )
        
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
        version: str,
        date_labels: List[str] = None
    ) -> List[Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
            
        if 'Normal day' not in date_labels:
            date_labels = ['Normal day'] + date_labels
        
        # Tự động tính historical months từ target_month
        historical_months, historical_year = self._calculate_historical_months(target_year, target_month)
        
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
        
        # Tính historical_start_date và historical_end_date để lưu vào metadata
        first_month = historical_months[0]
        last_month = historical_months[-1]
        first_month_year = historical_year
        
        # Nếu last_month < first_month thì đã cross-year
        if last_month < first_month:
            last_month_year = target_year
        else:
            last_month_year = historical_year
        
        historical_start_date = date(first_month_year, first_month, 1)
        # historical_end_date: ngày cuối cùng của tháng cuối cùng
        if last_month == 12:
            historical_end_date = date(last_month_year, 12, 31)
        else:
            next_month = date(last_month_year, last_month + 1, 1)
            historical_end_date = next_month - timedelta(days=1)
        
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
    
    def calculate_and_save_metadata(
        self,
        target_year: int,
        target_month: int,
        version: Optional[str] = None,
        date_labels: List[str] = None
    ) -> List[Dict]:
        if version is None:
            version = f"Thang {target_month}"
        
        metadata = self.calculate_metadata(
            target_year=target_year,
            target_month=target_month,
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
        target_month=1
    )
    
    print(f"Successfully saved {len(metadata)} metadata records")