**Run command**
- python -m src.etl.kpi_day
- python -m src.etl.day_actual_loader

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
  `created_at` DateTime DEFAULT now (),
  `updated_at` DateTime DEFAULT now ()
) ENGINE = ReplacingMergeTree (updated_at)
ORDER BY
  (year, month, calendar_date) SETTINGS index_granularity = 8192;

-- DDL để add column weighted_left sau cột weight
ALTER TABLE hskcdp.kpi_day ADD COLUMN `weighted_left` Decimal(40, 15) AFTER `weight`;

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