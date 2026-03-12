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
    
    def calculate_and_save_kpi_subchannel_metadata(
        self,
        target_year: int,
        target_month: int
    ) -> List[Dict]:
        # Lấy doanh thu 3 tháng gần nhất theo channel và subchannel
        revenue_by_subchannel_db = self.revenue_helper.get_subchannel_revenue_last_3_months()
        
        # Đảm bảo TẤT CẢ các channel đều có mặt để kpi_subchannel_initial không bị rỗng/thiếu hụt
        revenue_by_subchannel = {}
        for ch in self.constants.ALL_CHANNELS:
            revenue_by_subchannel[ch] = revenue_by_subchannel_db.get(ch, {})
            
        results = []
        data_to_insert = []
        now = datetime.now()
        
        for channel, subchannels in revenue_by_subchannel.items():
            # Tính tổng doanh thu của channel đó
            total_channel_revenue = sum(subchannels.values())
            
            # Nếu channel này KHÔNG TRẢ VỀ DATA (mới hoặc không có số), tự gán thành 1 subchannel mặc định 'Unknown' = 100%
            if total_channel_revenue == 0:
                subchannels = {'Unknown': 1.0}
                total_channel_revenue = 1.0
                
            for subchannel, revenue in subchannels.items():
                rev_pct = revenue / total_channel_revenue
                # rev_pct_adj ban đầu bằng rev_pct, có thể update sau nếu có logic thêm
                rev_pct_adj = rev_pct
                
                results.append({
                    'year': target_year,
                    'month': target_month,
                    'channel': channel,
                    'subchannel': subchannel,
                    'rev_pct': float(rev_pct),
                    'rev_pct_adj': float(rev_pct_adj)
                })
                
                data_to_insert.append([
                    target_year,
                    target_month,
                    channel,
                    subchannel,
                    float(rev_pct),
                    float(rev_pct_adj),
                    now,
                    now
                ])
                
        if data_to_insert:
            columns = [
                'year', 'month', 'channel', 'subchannel', 'rev_pct', 'rev_pct_adj',
                'created_at', 'updated_at'
            ]
            self.client.insert("hskcdp.kpi_subchannel_metadata", data_to_insert, column_names=columns)
                
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
