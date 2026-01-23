**Run command**
- python -m src.etl.month_base
- python -m src.etl.kpi_day_metadata
- python -m src.etl.kpi_month
- python -m src.etl.kpi_day

**Pipeline Workflow (Airflow Integration)**

### Overview
Pipeline tính toán KPI từ tháng (month) xuống ngày (day), với cả 2 phần đều chạy **hourly**.

### Dependencies và Thứ tự chạy:

#### 1. **month_base.py** (Chạy một lần hoặc khi cần update base KPI)
- **Mục đích**: Tính base KPI (`kpi_initial`) cho 12 tháng từ revenue 2025
- **Input**: `hskcdp.revenue_2025` (bảng tạm - cần refactor sang `object_sql_transactions`)
- **Output**: `hskcdp.kpi_month_base` (kpi_initial cho từng tháng)
- **Schedule**: One-time hoặc monthly (khi có thay đổi base KPI)
- **Dependencies**: Không có

#### 2. **kpi_day_metadata.py** (Chạy **monthly** - đầu mỗi tháng mới)
- **Mục đích**: Tính metadata (uplift, weight) cho các date_label dựa trên historical data
- **Input**: `object_sql_transactions` (3 tháng gần nhất)
- **Output**: `hskcdp.kpi_day_metadata` (uplift, weight cho từng date_label)
- **Schedule**: **Monthly** (chạy đầu mỗi tháng mới, vì 3 tháng gần nhất sẽ thay đổi)
  - Ví dụ: Tháng 1/2026 → lấy data từ 10, 11, 12/2025
  - Tháng 2/2026 → lấy data từ 11, 12/2025 và 1/2026 (cần chạy lại)
- **Dependencies**: Không có

#### 3. **kpi_month.py** (Chạy **hourly**)
- **Mục đích**: Tính `kpi_adjustment` cho các tháng, bao gồm EOM (End of Month)
- **Input**: 
  - `hskcdp.kpi_month_base` (kpi_initial)
  - `object_sql_transactions` (actual revenue)
- **Output**: `hskcdp.kpi_month` (kpi_initial, kpi_adjustment, eom)
- **Schedule**: **Hourly** (mỗi giờ chạy một lần)
- **Dependencies**: 
  - `month_base.py` (cần kpi_month_base)
  - `object_sql_transactions` (cần actual data)

#### 4. **kpi_day.py** (Chạy **hourly**)
- **Mục đích**: Tính `kpi_day_initial`, `kpi_day_adjustment`, và `eod` (End of Day) cho các ngày
- **Input**:
  - `hskcdp.kpi_month` (kpi_initial từ kpi_month)
  - `hskcdp.kpi_day_metadata` (uplift, weight)
  - `object_sql_transactions` (actual revenue)
- **Output**: `hskcdp.kpi_day` (kpi_day_initial, kpi_day_adjustment, eod)
- **Schedule**: **Hourly** (mỗi giờ chạy một lần)
- **Dependencies**:
  - `kpi_day_metadata.py` (cần uplift, weight)
  - `kpi_month.py` (cần kpi_initial từ kpi_month)
  - `object_sql_transactions` (cần actual data)

### Pipeline Flow Diagram:
```
month_base.py (one-time/monthly)
    ↓
kpi_day_metadata.py (monthly - đầu mỗi tháng mới)
    ↓
    ├─→ kpi_month.py (hourly) ──┐
    │                           │
    └─→ kpi_day.py (hourly) ←───┘
         (cần kpi_month.kpi_initial)
```

### Notes:
- `kpi_month.py` và `kpi_day.py` đều chạy **hourly** để cập nhật real-time
- `kpi_day_metadata.py` chạy **monthly** (đầu mỗi tháng mới) vì cần cập nhật 3 tháng gần nhất
- `kpi_day.py` phụ thuộc vào `kpi_month.py` (cần `kpi_month.kpi_initial`)
- Cả 2 đều query trực tiếp từ `object_sql_transactions` (không dùng staging tables nữa)
- `month_base.py` và `kpi_day_metadata.py` có thể chạy song song (không phụ thuộc nhau)

**Formular**
1. Cách tính kpi_day_adjustment

- Uplift = Hệ số so với ngày NormalDay

- Weighted = Uplift * SoNgay

- Weighted Left = ?

- kpi_initial_day = Uplift * kpi_month / SUM(weight)

- SUM(Weighted Left)= "Weighted Left" đang là tổng uplift của các ngày chưa có actual

- kpi_day_adjustment = kpi_initial_day - (SUM(Gap) * Weighted / SUM(Weighted Left))

<!-- kpi_brand = kpi_day * kpi_channel -->


2. EOM - KPI MONTH
- EOM = Actual **(Sum(actual))** + Sum(rev eom những ngày còn lại)

- Sum(rev eom những ngày còn lại) = Avg rev of normal day **(30 ngày gần nhất)** * uplift | Ví dụ đang tính cho ngày 10, thì còn lại 20 ngày, tính xem trong 20 ngày đó có những loại ngày nào? bao nhiêu ngày? rồi SUM(normal_day * uplift)

- Giá trị của kpi_adjustment = eom



**- DDL CREATE ALL TABLES IN PROJECT**
CREATE TABLE hskcdp.kpi_day (
  `calendar_date` Date,
  `year` UInt16,
  `month` UInt8,
  `day` UInt8,
  `date_label` String,
  `kpi_month` Decimal(40, 15),
  `uplift` Decimal(40, 15),
  `weight` Decimal(40, 15),
  `weighted_left` Decimal(40, 15),
  `total_weight_month` Decimal(40, 15),
  `kpi_day_initial` Decimal(40, 15),
  `actual` Nullable(Decimal(40, 15)),
  `gap` Nullable(Decimal(40, 15)),
  `kpi_day_adjustment` Nullable(Decimal(40, 15)),
  `eod` Nullable(Decimal(40, 15)),
  `created_at` DateTime DEFAULT now (),
  `updated_at` DateTime DEFAULT now ()
) ENGINE = ReplacingMergeTree (updated_at)
ORDER BY
  (year, month, calendar_date) SETTINGS index_granularity = 8192;

-- DDL để add column weighted_left sau cột weight
ALTER TABLE hskcdp.kpi_day ADD COLUMN `weighted_left` Decimal(40, 15) AFTER `weight`;

-- DDL để add column eod sau cột kpi_day_adjustment
ALTER TABLE hskcdp.kpi_day ADD COLUMN `eod` Nullable(Decimal(40, 15)) AFTER `kpi_day_adjustment`;

CREATE TABLE hskcdp.kpi_day_channel (
  `calendar_date` Date,
  `year` UInt16,
  `month` UInt8,
  `day` UInt8,
  `date_label` String,
  `channel` String,
  `revenue_percentage` Decimal(40, 15),
  `kpi_day_channel_initial` Decimal(40, 15),
  `actual` Nullable(Decimal(40, 15)),
  `gap` Nullable(Decimal(40, 15)),
  `kpi_adjustment` Nullable(Decimal(40, 15)),
  `created_at` DateTime DEFAULT now(),
  `updated_at` DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year, month, calendar_date, channel)
SETTINGS index_granularity = 8192;

-- DDL để add columns actual, gap, kpi_adjustment nếu bảng đã tồn tại
ALTER TABLE hskcdp.kpi_day_channel ADD COLUMN `actual` Nullable(Decimal(40, 15)) AFTER `kpi_day_channel_initial`;
ALTER TABLE hskcdp.kpi_day_channel ADD COLUMN `gap` Nullable(Decimal(40, 15)) AFTER `actual`;
ALTER TABLE hskcdp.kpi_day_channel ADD COLUMN `kpi_adjustment` Nullable(Decimal(40, 15)) AFTER `gap`;

CREATE TABLE hskcdp.kpi_day_channel_brand (
  `calendar_date` Date,
  `year` UInt16,
  `month` UInt8,
  `day` UInt8,
  `date_label` String,
  `channel` String,
  `brand_name` String,
  `percentage_of_revenue_by_brand` Decimal(40, 15),
  `kpi_brand_initial` Decimal(40, 15),
  `actual` Nullable(Decimal(40, 15)),
  `gap` Nullable(Decimal(40, 15)),
  `kpi_brand_adjustment` Nullable(Decimal(40, 15)),
  `created_at` DateTime DEFAULT now(),
  `updated_at` DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year, month, calendar_date, channel, brand_name)
SETTINGS index_granularity = 8192;

-- DDL để add columns actual, gap, kpi_brand_adjustment nếu bảng đã tồn tại
ALTER TABLE hskcdp.kpi_day_channel_brand ADD COLUMN `actual` Nullable(Decimal(40, 15)) AFTER `kpi_brand_initial`;
ALTER TABLE hskcdp.kpi_day_channel_brand ADD COLUMN `gap` Nullable(Decimal(40, 15)) AFTER `actual`;
ALTER TABLE hskcdp.kpi_day_channel_brand ADD COLUMN `kpi_brand_adjustment` Nullable(Decimal(40, 15)) AFTER `gap`;

CREATE TABLE hskcdp.actual_2026_day_staging (
  `year` UInt16,
  `calendar_date` Date,
  `month` UInt8,
  `day` UInt8,
  `actual_amount` Decimal(40, 15),
  `processed` UInt8,
  `processed_at` Nullable(DateTime),
  `created_at` DateTime DEFAULT now(),
  `updated_at` DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year, calendar_date)
SETTINGS index_granularity = 8192;



**Logic**
1. Các số doanh thu thực tế (doanh thu tháng 1, doanh thu ngày 01/01/2026,...) sau này sẽ được lấy hết ở script tracsactions | tracsaction_details,... 


**Work flow**
1. base.py
- Tính toán kpi_initial cho Tháng 1.

2. actual_loader.py
- Insert doanh thu thực tế của các tháng vào.

3. kpi_month.py
- Tính toán kpi_adjustment cho các tháng.



**Command**
python -m src.etl.month_base
python -m src.etl.kpi_month 
python -m src.etl.kpi_day_metadata
python -m src.etl.kpi_day
python -m src.etl.kpi_channel_metadata
python -m src.etl.kpi_channel
python -m src.etl.kpi_brand_metadata
python -m src.etl.kpi_brand