from decimal import Decimal
from datetime import datetime, date
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
    ) -> Dict[str, Dict[str, Decimal]]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
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
                
                channel_percentage[date_label][channel] = Decimal(percentage)
        
        return channel_percentage
    
    def calculate_kpi_day_channel_metadata(
        self,
        target_year: int,
        target_month: int,
        date_labels: List[str] = None
    ) -> List[Dict]:
        if date_labels is None:
            date_labels = self.constants.DATE_LABELS
        
        channel_percentage = self.calculate_channel_revenue_percentage(
            date_labels=date_labels
        )
        
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
            
            channels_for_label = channel_percentage.get(date_label, {})
            
            for channel in self.constants.ALL_CHANNELS:
                percentage = channels_for_label.get(channel, 0.0)
                
                rev_pct_adjustment = percentage
                
                results.append({
                    'calendar_date': calendar_date,
                    'year': year,
                    'month': month,
                    'day': day,
                    'date_label': date_label,
                    'channel': channel,
                    'rev_pct': Decimal(str(percentage)),
                    'rev_pct_adjustment': Decimal(str(rev_pct_adjustment))
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
                row['rev_pct'],
                row['rev_pct_adjustment'],
                now,
                now
            ])
        
        columns = [
            'calendar_date', 'year', 'month', 'day', 'date_label',
            'channel', 'rev_pct', 'rev_pct_adjustment',
            'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_channel_metadata", data, column_names=columns)
    
    def check_metadata_annually_exists(
        self,
        target_year: int,
        target_month: int
    ) -> bool:
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
        query = f"""
            SELECT year, month, priority_label, pct_offline, pct_online, pct_ecom
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
                'pct_offline': Decimal(row[3]) if row[3] is not None else 0.0,
                'pct_online': Decimal(row[4]) if row[4] is not None else 0.0,
                'pct_ecom': Decimal(row[5]) if row[5] is not None else 0.0
            })
        
        return data
    
    def update_channel_metadata_from_annually(
        self,
        target_year: int,
        target_month: int
    ) -> None:
        annually_data = self.get_metadata_annually_data(target_year, target_month)
        
        if not annually_data:
            return
        
        for row in annually_data:
            priority_label = row['priority_label'].replace("'", "''")
            pct_offline = row['pct_offline']
            pct_online = row['pct_online']
            pct_ecom = row['pct_ecom']
            
            update_offline_query = f"""
                ALTER TABLE hskcdp.kpi_channel_metadata
                UPDATE 
                    rev_pct = toDecimal64({pct_offline}, 15),
                    rev_pct_adjustment = toDecimal64({pct_offline}, 15)
                WHERE year = {target_year}
                  AND month = {target_month}
                  AND date_label = '{priority_label}'
                  AND channel = 'OFFLINE_HASAKI'
            """
            self.client.command(update_offline_query)
            
            update_online_query = f"""
                ALTER TABLE hskcdp.kpi_channel_metadata
                UPDATE 
                    rev_pct = toDecimal64({pct_online}, 15),
                    rev_pct_adjustment = toDecimal64({pct_online}, 15)
                WHERE year = {target_year}
                  AND month = {target_month}
                  AND date_label = '{priority_label}'
                  AND channel = 'ONLINE_HASAKI'
            """
            self.client.command(update_online_query)
            
            update_ecom_query = f"""
                ALTER TABLE hskcdp.kpi_channel_metadata
                UPDATE 
                    rev_pct = toDecimal64({pct_ecom}, 15),
                    rev_pct_adjustment = toDecimal64({pct_ecom}, 15)
                WHERE year = {target_year}
                  AND month = {target_month}
                  AND date_label = '{priority_label}'
                  AND channel = 'ECOM'
            """
            self.client.command(update_ecom_query)
    
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
        
        if self.check_metadata_annually_exists(target_year, target_month):
            self.update_channel_metadata_from_annually(target_year, target_month)
        
        return metadata_data


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPIDayChannelMetadataCalculator(constants)
    
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
            target_month = today.month
        else:
            target_month = 1
    
    if target_month < 1 or target_month > 12:
        print(f"Error: target_month must be between 1 and 12, received: {target_month}")
        sys.exit(1)
    
    print(f"Calculating kpi_day_channel_metadata for month {target_month}/{target_year}...")
    metadata_data = calculator.calculate_and_save_kpi_day_channel_metadata(
        target_year=target_year,
        target_month=target_month
    )
    
    print(f"Successfully saved {len(metadata_data)} kpi_channel_metadata records")

