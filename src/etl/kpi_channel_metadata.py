from datetime import datetime
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIDayChannelMetadataCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def calculate_channel_revenue_percentage(
        self,
        date_labels: List[str] = None
    ) -> Dict[str, Dict[str, float]]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
        if 'Normal day' not in date_labels:
            date_labels = ['Normal day'] + date_labels
        
        # Sử dụng helper methods để query từ transactions (3 tháng gần nhất)
        total_revenue_by_label = self.revenue_helper.get_total_revenue_by_date_label_last_3_months(
            date_labels
        )
        
        channel_revenue_by_label = self.revenue_helper.get_revenue_by_date_label_and_channel_from_platform_last_3_months(
            date_labels
        )

        channel_percentage = {}
        
        for date_label, channels in channel_revenue_by_label.items():
            total_revenue = total_revenue_by_label.get(date_label, 0)
            
            if date_label not in channel_percentage:
                channel_percentage[date_label] = {}
            
            for channel, revenue in channels.items():
                if total_revenue > 0:
                    percentage = revenue / total_revenue
                else:
                    percentage = 0.0
                
                channel_percentage[date_label][channel] = float(percentage)
        
        return channel_percentage
    
    def calculate_kpi_day_channel_metadata(
        self,
        target_year: int,
        target_month: int,
        date_labels: List[str] = None
    ) -> List[Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
        if 'Normal day' not in date_labels:
            date_labels = ['Normal day'] + date_labels
        
        # Get channel revenue percentage (tự động dùng 3 tháng gần nhất)
        channel_percentage = self.calculate_channel_revenue_percentage(
            date_labels=date_labels
        )
        
        # Get all dates for target month from dim_date (sử dụng helper)
        dim_dates = self.revenue_helper.get_dim_dates_for_month_excluding_double_days(
            target_year=target_year,
            target_month=target_month
        )
        
        results = []
        
        for dim_date in dim_dates:
            calendar_date = dim_date['calendar_date']
            year = dim_date['year']
            month = dim_date['month']
            day = dim_date['day']
            date_label = dim_date['date_label']
            
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
        date_labels: List[str] = None
    ) -> List[Dict]:
        metadata_data = self.calculate_kpi_day_channel_metadata(
            target_year=target_year,
            target_month=target_month,
            date_labels=date_labels
        )
        
        self.save_kpi_day_channel_metadata(metadata_data)
        
        return metadata_data


if __name__ == "__main__":
    constants = Constants()
    calculator = KPIDayChannelMetadataCalculator(constants)
    
    print("Calculating kpi_day_channel_metadata for month 1/2026...")
    metadata_data = calculator.calculate_and_save_kpi_day_channel_metadata(
        target_year=2026,
        target_month=1
    )
    
    print(f"Successfully saved {len(metadata_data)} kpi_day_channel_metadata records")

