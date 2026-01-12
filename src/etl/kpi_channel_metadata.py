from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants


class KPIDayChannelMetadataCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
    
    def calculate_channel_revenue_percentage(
        self,
        historical_months: List[int],
        historical_year: int,
        date_labels: List[str] = None
    ) -> Dict[str, Dict[str, float]]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
        if 'Normal day' not in date_labels:
            date_labels = ['Normal day'] + date_labels
        
        historical_start_date = date(historical_year, min(historical_months), 1) # [10, 11, 12] -> 01/10/2025 
        
        if max(historical_months) == 12:
            historical_end_date = date(historical_year, 12, 31) # 31/12/2025
        else:
            if max(historical_months) + 1 <= 12:
                next_month = date(historical_year, max(historical_months) + 1, 1) # 01/11/2025
            else:
                next_month = date(historical_year + 1, 1, 1) # 01/01/2026
            historical_end_date = next_month - timedelta(days=1) # 30/11/2025
        
        # Calculate total revenue by date_label (excluding specific double days)
        total_revenue_query_by_date_label = f"""
            SELECT 
                date_label, 
                SUM(revenue) as total_revenue 
            FROM hskcdp.revenue_2025_ver2
            WHERE calendar_date >= '{historical_start_date}'
              AND calendar_date <= '{historical_end_date}'
              AND date_label IN ({','.join([f"'{dl}'" for dl in date_labels])})
              AND NOT (
                  (toMonth(calendar_date) = 6 AND toDayOfMonth(calendar_date) BETWEEN 5 AND 7) OR
                  (toMonth(calendar_date) = 9 AND toDayOfMonth(calendar_date) BETWEEN 8 AND 10) OR
                  (toMonth(calendar_date) = 11 AND toDayOfMonth(calendar_date) BETWEEN 10 AND 12) OR
                  (toMonth(calendar_date) = 12 AND toDayOfMonth(calendar_date) BETWEEN 11 AND 13)
              )
            GROUP BY date_label
        """
        
        total_result = self.client.query(total_revenue_query_by_date_label)
        total_revenue_by_label = {row[0]: float(row[1]) for row in total_result.result_rows}
        
        # Calculate revenue by channel and date_label
        channel_revenue_query_by_date_label_and_channel = f"""
            SELECT 
                date_label,
                channel,
                SUM(revenue) as revenue
            FROM hskcdp.revenue_2025_ver2
            WHERE calendar_date >= '{historical_start_date}'
              AND calendar_date <= '{historical_end_date}'
              AND date_label IN ({','.join([f"'{dl}'" for dl in date_labels])})
              AND NOT (
                  (toMonth(calendar_date) = 6 AND toDayOfMonth(calendar_date) BETWEEN 5 AND 7) OR
                  (toMonth(calendar_date) = 9 AND toDayOfMonth(calendar_date) BETWEEN 8 AND 10) OR
                  (toMonth(calendar_date) = 11 AND toDayOfMonth(calendar_date) BETWEEN 10 AND 12) OR
                  (toMonth(calendar_date) = 12 AND toDayOfMonth(calendar_date) BETWEEN 11 AND 13)
              )
            GROUP BY date_label, channel
        """
        
        channel_result = self.client.query(channel_revenue_query_by_date_label_and_channel)

        channel_percentage = {}
        
        for row in channel_result.result_rows:
            date_label = row[0]
            channel = row[1]
            revenue = float(row[2])
            total_revenue = total_revenue_by_label.get(date_label, 0)
            
            if total_revenue > 0:
                percentage = revenue / total_revenue
            else:
                percentage = 0.0
            
            if date_label not in channel_percentage:
                channel_percentage[date_label] = {}
            
            channel_percentage[date_label][channel] = float(percentage)
        
        return channel_percentage
    
    def calculate_kpi_day_channel_metadata(
        self,
        target_year: int,
        target_month: int,
        historical_months: Optional[List[int]] = None,
        historical_year: Optional[int] = None
    ) -> List[Dict]:
        if historical_year is None:
            historical_year = self.constants.BASE_YEAR
        
        if historical_months is None:
            # Use 3 months before target_month
            if target_month <= 3:
                # If target_month is 1, 2, or 3, use last 3 months of previous year
                historical_months = [10, 11, 12]
                historical_year = self.constants.BASE_YEAR
            else:
                historical_months = [target_month - 3, target_month - 2, target_month - 1]
        
        # Get channel revenue percentage
        channel_percentage = self.calculate_channel_revenue_percentage(
            historical_months=historical_months,
            historical_year=historical_year
        )
        
        # Get all dates for target month from dim_date
        dim_date_query = f"""
            SELECT 
                calendar_date,
                year,
                month,
                day,
                date_label
            FROM dim_date
            WHERE year = {target_year}
              AND month = {target_month}
              AND NOT (
                  (month = 6 AND day BETWEEN 5 AND 7) OR
                  (month = 9 AND day BETWEEN 8 AND 10) OR
                  (month = 11 AND day BETWEEN 10 AND 12) OR
                  (month = 12 AND day BETWEEN 11 AND 13)
              )
            ORDER BY calendar_date
        """
        
        dim_date_result = self.client.query(dim_date_query)
        
        results = []
        
        for row in dim_date_result.result_rows:
            calendar_date = row[0]
            year = int(row[1])
            month = int(row[2])
            day = int(row[3])
            date_label = str(row[4])
            
            # Get channel percentages for this date_label
            channels_for_label = channel_percentage.get(date_label, {})
            
            if not channels_for_label:
                # If no channel data for this date_label, skip
                continue
            
            # Create metadata for each channel
            for channel, percentage in channels_for_label.items():
                # Initially, revenue_percentage_adj equals revenue_percentage
                # This can be adjusted later based on actual data or other factors
                revenue_percentage_adj = percentage
                
                results.append({
                    'calendar_date': calendar_date,
                    'year': year,
                    'month': month,
                    'day': day,
                    'date_label': date_label,
                    'channel': channel,
                    'revenue_percentage': float(percentage),
                    'revenue_percentage_adj': float(revenue_percentage_adj)
                })
        
        return results
    
    def save_kpi_day_channel_metadata(self, metadata_data: List[Dict]) -> None:
        if not metadata_data:
            return
        
        now = datetime.now()
        
        data = []
        for row in metadata_data:
            data.append([
                row['calendar_date'],
                row['year'],
                row['month'],
                row['day'],
                row['date_label'],
                row['channel'],
                row['revenue_percentage'],
                row['revenue_percentage_adj'],
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'day', 'date_label',
            'channel', 'revenue_percentage', 'revenue_percentage_adj',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_day_channel_metadata", data, column_names=columns)
    
    def calculate_and_save_kpi_day_channel_metadata(
        self,
        target_year: int,
        target_month: int,
        historical_months: Optional[List[int]] = None,
        historical_year: Optional[int] = None
    ) -> List[Dict]:
        metadata_data = self.calculate_kpi_day_channel_metadata(
            target_year=target_year,
            target_month=target_month,
            historical_months=historical_months,
            historical_year=historical_year
        )
        
        self.save_kpi_day_channel_metadata(metadata_data)
        
        return metadata_data


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIDayChannelMetadataCalculator(constants)
    
    print("Calculating kpi_day_channel_metadata for month 1/2026...")
    metadata_data = calculator.calculate_and_save_kpi_day_channel_metadata(
        target_year=2026,
        target_month=1,
        historical_months=[10, 11, 12],
        historical_year=2025
    )
    
    print(f"Successfully saved {len(metadata_data)} kpi_day_channel_metadata records")

