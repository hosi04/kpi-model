# Pipeline Tính Toán KPI Day (Slide Version)

## Tổng quan
File `kpi_day.py` tính toán `kpi_day_initial`, `kpi_day_adjustment`, và `eod` (End of Day) cho các ngày trong tháng.

## Pipeline Flow (Tóm tắt)

```
1. Tính kpi_day_initial
   ├─→ Lấy kpi_month (kpi_initial) từ kpi_month
   ├─→ Lấy uplift, weight từ kpi_day_metadata
   └─→ kpi_day_initial = (uplift × kpi_month) / total_weight_month
   ↓
2. Tính kpi_day_adjustment
   ├─→ Lấy actual theo ngày từ transactions
   ├─→ Tính gap = actual - kpi_day_initial (cho ngày có actual)
   ├─→ Tính gap = -kpi_day_initial (cho ngày đã qua không có actual)
   ├─→ Phân bổ gap cho ngày chưa có actual
   └─→ kpi_day_adjustment = kpi_day_initial - (gap × uplift / SUM(weighted_left))
   ↓
3. Tính EOD (cho ngày hiện tại)
   ├─→ Lấy % doanh thu theo giờ (30 ngày gần nhất)
   ├─→ Lấy actual từ 0h đến giờ hiện tại
   └─→ EOD = actual / sum(% của các giờ đã qua)
   ↓
4. Lưu kết quả vào kpi_day
```

---

## Các Main Query Chính

### 1. **Lấy kpi_month và kpi_day_metadata để tính kpi_day_initial**
```sql
SELECT 
    d.calendar_date,
    d.date_label,
    m.kpi_initial AS kpi_month,
    md.uplift,
    md.weight,
    md.total_weight_month
FROM dim_date d
INNER JOIN (
    SELECT year, month, kpi_initial,
           row_number() OVER (PARTITION BY year, month, version ORDER BY updated_at DESC) AS rn
    FROM hskcdp.kpi_month
    WHERE version = 'Thang 1'
) AS m ON d.year = m.year AND d.month = m.month AND m.rn = 1
INNER JOIN (
    SELECT year, month, date_label, uplift, weight, total_weight_month,
           row_number() OVER (PARTITION BY year, month, date_label ORDER BY updated_at DESC) AS rn
    FROM hskcdp.kpi_day_metadata
) AS md ON d.year = md.year AND d.month = md.month 
       AND d.date_label = md.date_label AND md.rn = 1
WHERE d.year = 2026 AND d.month = 1
```
**Tác dụng:** Lấy dữ liệu cần thiết để tính `kpi_day_initial` cho từng ngày trong tháng

---

### 2. **Lấy actual theo ngày từ transactions**
```sql
SELECT 
    toDate(t.created_at) as calendar_date,
    SUM(t.transaction_total) as actual_amount
FROM hskcdp.object_sql_transactions AS t FINAL
WHERE toYear(t.created_at) = 2026
  AND toMonth(t.created_at) = 1
  AND t.status NOT IN ('Canceled', 'Cancel')
GROUP BY calendar_date
```
**Tác dụng:** Lấy actual revenue theo từng ngày trong tháng để tính gap và kpi_day_adjustment

---

### 3. **Lấy % doanh thu theo giờ (30 ngày gần nhất) - cho EOD**
```sql
SELECT 
    toHour(t.created_at) as hour,
    SUM(t.transaction_total) as hour_revenue
FROM hskcdp.object_sql_transactions AS t FINAL
WHERE toDate(t.created_at) >= today() - INTERVAL 30 DAY
  AND t.status NOT IN ('Canceled', 'Cancel')
GROUP BY hour
ORDER BY hour
```
**Tác dụng:** Tính % doanh thu cho từng giờ (0-23h) trong 30 ngày gần nhất, dùng để tính EOD

---

### 4. **Lấy actual từ 0h đến giờ hiện tại - cho EOD**
```sql
SELECT 
    SUM(t.transaction_total) as actual_amount
FROM hskcdp.object_sql_transactions AS t FINAL
WHERE toDate(t.created_at) = '2026-01-15'
  AND toHour(t.created_at) < 9  -- Ví dụ: chạy lúc 9h thì lấy từ 0h đến <9h
  AND t.status NOT IN ('Canceled', 'Cancel')
```
**Tác dụng:** Lấy actual revenue từ đầu ngày đến giờ hiện tại (exclusive) để tính EOD

---

## Công Thức Tính Toán

### kpi_day_initial
```
kpi_day_initial = (uplift × kpi_month) / total_weight_month

Trong đó:
- uplift = Hệ số so với Normal day (từ kpi_day_metadata)
- kpi_month = kpi_initial từ kpi_month
- total_weight_month = Tổng weight của cả tháng
```

### kpi_day_adjustment
```
Ngày có actual:
  kpi_day_adjustment = actual
  gap = actual - kpi_day_initial

Ngày đã qua và không có actual:
  kpi_day_adjustment = 0
  gap = -kpi_day_initial

Ngày chưa qua và không có actual:
  kpi_day_adjustment = kpi_day_initial - (SUM(Gap) × uplift / SUM(weighted_left))
  gap = None
```

### EOD (End of Day)
```
EOD = actual_until_hour / sum(% của các giờ đã qua)

Trong đó:
- actual_until_hour = Actual từ 0h00 đến <giờ hiện tại>
- sum(% của các giờ đã qua) = Tổng % doanh thu từ 0h đến (current_hour - 1)
```

---

## Output

**Bảng:** `hskcdp.kpi_day`

**Các cột chính:**
- `calendar_date`: Ngày
- `kpi_month`: kpi_initial từ kpi_month
- `uplift`, `weight`: Từ kpi_day_metadata
- `kpi_day_initial`: KPI ban đầu của ngày
- `actual`: Actual revenue (từ transactions)
- `gap`: Gap = actual - kpi_day_initial
- `kpi_day_adjustment`: KPI sau điều chỉnh
- `weighted_left`: uplift (nếu chưa có actual và chưa qua), 0 (nếu có actual hoặc đã qua)
- `eod`: End of Day (chỉ có giá trị cho ngày hiện tại, None cho các ngày khác)
