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

    def calculate_and_save_kpi_day_channel_metadata(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        date_labels = self.constants.DATE_LABELS

        total_rev_by_label = self.revenue_helper.get_total_revenue_by_date_label_last_3_months(date_labels)
        channel_rev_by_label = self.revenue_helper.get_revenue_by_date_label_and_channel_from_platform_last_3_months(date_labels)
        
        # Get metadata_annually (replace for UPDATE then)
        annually_query = f"""
            SELECT 
                priority_label, 
                pct_offline, 
                pct_online, pct_ecom
            FROM hskcdp.metadata_annually
            WHERE year = {target_year} 
              AND month = {target_month}
        """
        annually_rows = self.client.query(annually_query).result_rows
        annually_map = {
            row[0]: {
                'OFFLINE_HASAKI': Decimal(str(row[1])),
                'ONLINE_HASAKI': Decimal(str(row[2])),
                'ECOM': Decimal(str(row[3]))
            } for row in annually_rows
        }

        # Build mapping pct for each date_label
        label_channel_pct = {}
        for label in date_labels:
            total = total_rev_by_label.get(label, 0)
            label_channel_pct[label] = {}
            
            # Priority get from metadata_annually
            if label in annually_map:
                label_channel_pct[label] = annually_map[label]
            else:
                # If not in annually, calculate from historical data
                for channel in self.constants.ALL_CHANNELS:
                    rev = channel_rev_by_label.get(label, {}).get(channel, 0.0)
                    if total > 0:
                        pct = Decimal(str(rev)) / Decimal(str(total))
                    else:
                        pct = Decimal('0')
                    label_channel_pct[label][channel] = pct

        # Apply scale for dim_dates of target month
        dim_dates = self.revenue_helper.get_dim_dates_for_month_excluding_double_days(target_year, target_month)
        
        results = []
        data_to_insert = []
        now = datetime.now()

        for dim_date in dim_dates:
            date_label = dim_date['date_label']
            pct_map = label_channel_pct.get(date_label, {})
            
            for channel in self.constants.ALL_CHANNELS:
                pct = pct_map.get(channel, Decimal('0'))
                
                results.append({
                    'calendar_date': dim_date['calendar_date'],
                    'year': dim_date['year'],
                    'month': dim_date['month'],
                    'day': dim_date['day'],
                    'date_label': date_label,
                    'channel': channel,
                    'rev_pct': pct,
                    'rev_pct_adjustment': pct
                })
                
                data_to_insert.append([
                    dim_date['calendar_date'], dim_date['year'], dim_date['month'], dim_date['day'],
                    date_label, channel, pct, pct, now, now
                ])

        # Save to ClickHouse
        if data_to_insert:
            columns = [
                'calendar_date', 'year', 'month', 'day', 'date_label',
                'channel', 'rev_pct', 'rev_pct_adjustment',
                'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_channel_metadata", data_to_insert, column_names=columns)
            print(f"[INFO] Successfully inserted {len(data_to_insert)} records into kpi_channel_metadata")

        return results


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
                target_month = int(sys.argv[i + 1]); i += 2
            elif sys.argv[i] == "--target-year" and i + 1 < len(sys.argv):
                target_year = int(sys.argv[i + 1]); i += 2
            else: i += 1
    
    if target_month is None:
        today = date.today()
        target_month = today.month + 1
        if target_month > 12:
            target_month = 1; target_year = today.year + 1
        else: target_year = today.year
    
    print(f"Calculating kpi_day_channel_metadata for month {target_month}/{target_year}...")
    calculator.calculate_and_save_kpi_day_channel_metadata(target_year, target_month)
