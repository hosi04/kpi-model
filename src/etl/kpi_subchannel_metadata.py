from decimal import Decimal
from datetime import datetime, date
from typing import List, Dict
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPISubChannelMetadataCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
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
            SELECT 
                year, 
                month, 
                priority_label, 
                pct_shopee, 
                pct_tiktok, 
                pct_lazada
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
                'pct_shopee': Decimal(str(row[3])) if row[3] is not None else Decimal('0'),
                'pct_tiktok': Decimal(str(row[4])) if row[4] is not None else Decimal('0'),
                'pct_lazada': Decimal(str(row[5])) if row[5] is not None else Decimal('0')
            })
        
        return data

    def update_subchannel_metadata_from_annually(
        self,
        target_year: int,
        target_month: int
    ) -> None:
        annually_data = self.get_metadata_annually_data(target_year, target_month)
        
        if not annually_data:
            return
        
        for row in annually_data:
            priority_label = row['priority_label'].replace("'", "''")
            
            ecom_mappings = {
                'Shopee': row['pct_shopee'],
                'Tiktok': row['pct_tiktok'],
                'Lazada': row['pct_lazada']
            }
            
            # Validate tổng pct
            total_pct = sum(ecom_mappings.values())
            if total_pct == 0:
                continue
            
            # Bước 1: Xóa row ECOM cũ
            delete_query = f"""
                ALTER TABLE hskcdp.kpi_subchannel_metadata
                DELETE
                WHERE year = {target_year}
                AND month = {target_month}
                AND date_label = '{priority_label}'
                AND channel = 'ECOM'
            """
            self.client.command(delete_query)
            
            # Bước 2: Insert đúng subchannels từ metadata_annually
            now = datetime.now()
            rows_to_insert = []
            for subchannel, pct in ecom_mappings.items():
                if pct > 0:
                    rows_to_insert.append([
                        target_year, target_month, priority_label,
                        'ECOM', subchannel, subchannel, # store_name = subchannel for ECOM
                        float(pct), float(pct),
                        now, now
                    ])
            
            if rows_to_insert:
                self.client.insert(
                    "hskcdp.kpi_subchannel_metadata",
                    rows_to_insert,
                    column_names=[
                        'year', 'month', 'date_label', 'channel', 'subchannel', 'store_name',
                        'rev_pct', 'rev_pct_adj', 'created_at', 'updated_at'
                    ]
                )
                print(f"[INFO] Updated ECOM subchannels for '{priority_label}': {list(ecom_mappings.keys())}")


    def calculate_and_save_kpi_subchannel_metadata(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        dim_dates = self.revenue_helper.get_dim_dates_for_month_excluding_double_days(target_year, target_month)
        actual_labels = list(set(d['date_label'] for d in dim_dates))
        
        if not actual_labels:
            print(f"No labels found for month {target_month}/{target_year}.")
            return []

        revenue_by_subchannel_db = self.revenue_helper.get_subchannel_revenue_last_3_months(actual_labels)
        
        results = []
        data_to_insert = []
        now = datetime.now()
        
        for date_label in actual_labels:
            db_label_data = revenue_by_subchannel_db.get(date_label, {})
            
            for channel in self.constants.ALL_CHANNELS:
                channel_data = db_label_data.get(channel, {})
                
                # Tính tổng channel revenue trực tiếp từ các store
                total_channel_revenue = 0
                for stores in channel_data.values():
                    total_channel_revenue += sum(stores.values())
                
                if total_channel_revenue == 0:
                    # Fallback
                    items = [('Unknown', 'Unknown', 1.0)]
                    total_channel_revenue = 1.0
                else:
                    items = []
                    for subchannel, stores in channel_data.items():
                        for store_name, revenue in stores.items():
                            items.append((subchannel, store_name, revenue))
                    
                for subchannel, store_name, revenue in items:
                    rev_pct = revenue / total_channel_revenue
                    rev_pct_adj = rev_pct
                    
                    results.append({
                        'year': target_year,
                        'month': target_month,
                        'date_label': date_label,
                        'channel': channel,
                        'subchannel': subchannel,
                        'store_name': store_name,
                        'rev_pct': float(rev_pct),
                        'rev_pct_adj': float(rev_pct_adj)
                    })
                    
                    data_to_insert.append([
                        target_year,
                        target_month,
                        date_label,
                        channel,
                        subchannel,
                        store_name,
                        float(rev_pct),
                        float(rev_pct_adj),
                        now,
                        now
                    ])
                    
        if data_to_insert:
            columns = [
                'year', 'month', 'date_label', 'channel', 'subchannel', 'store_name',
                'rev_pct', 'rev_pct_adj', 'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_subchannel_metadata", data_to_insert, column_names=columns)

        if self.check_metadata_annually_exists(target_year, target_month):
            print(f"Found annual metadata for {target_month}/{target_year}, updating ECOM subchannels...")
            self.update_subchannel_metadata_from_annually(target_year, target_month)
                
        return results


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPISubChannelMetadataCalculator(constants)
    
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
        target_month = today.month + 1
        if target_month > 12:
            target_month = 1
            target_year = today.year + 1
        else:
            target_year = today.year
    
    if target_month < 1 or target_month > 12:
        print(f"Error: target_month must be between 1 and 12, received: {target_month}")
        sys.exit(1)
    
    print(f"Calculating kpi_subchannel_metadata for month {target_month}/{target_year}...")
    metadata_data = calculator.calculate_and_save_kpi_subchannel_metadata(
        target_year=target_year,
        target_month=target_month
    )
    
    print(f"Successfully saved {len(metadata_data)} kpi_subchannel_metadata records")
