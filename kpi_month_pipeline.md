# Pipeline Tính Toán KPI Month (Slide Version)

## Tổng quan
File `kpi_month.py` tính toán `kpi_adjustment` cho các tháng, bao gồm EOM (End of Month) và phân bổ gap cho các tháng chưa có actual.

## Pipeline Flow (Tóm tắt)

```
1. Xác định tháng đang tính
   ↓
2. Lấy kpi_initial (base KPI)
   ↓
3. Tính EOM cho các tháng có actual
   ├─→ Sum(actual) từ transactions
   ├─→ Số ngày còn lại theo date_label
   └─→ Sum(rev eom) = Σ(ngày_còn_lại × avg_normal_day × uplift)
   ↓
4. Tính gap và phân bổ cho tháng chưa có actual
   ↓
5. Lưu kết quả vào kpi_month
```

---

## Các Main Query Chính

### 1. **Lấy kpi_initial (Base KPI)**
```sql
SELECT year, month, kpi_initial
FROM hskcdp.kpi_month_base
WHERE year = 2026
```
**Tác dụng:** Lấy base KPI cho 12 tháng (được tính từ `month_base.py`)

---

### 2. **Lấy Sum(actual) theo ngày**
```sql
SELECT SUM(t.transaction_total) as sum_actual
FROM hskcdp.object_sql_transactions AS t FINAL
WHERE toYear(t.created_at) = 2026
  AND toMonth(t.created_at) = 1
  AND t.status NOT IN ('Canceled', 'Cancel')
```
**Tác dụng:** Tính tổng actual revenue của tháng từ transactions

---

### 3. **Lấy avg revenue Normal day (30 ngày gần nhất)**
```sql
SELECT AVG(daily_revenue) AS avg_rev_normal_day
FROM (
    SELECT toDate(t.created_at) as calendar_date,
           SUM(t.transaction_total) AS daily_revenue
    FROM hskcdp.object_sql_transactions AS t FINAL
    INNER JOIN hskcdp.dim_date d ON toDate(t.created_at) = d.calendar_date
    WHERE d.date_label = 'Normal day'
      AND toDate(t.created_at) >= today() - 30
      AND t.status NOT IN ('Canceled', 'Cancel')
    GROUP BY calendar_date
)
```
**Tác dụng:** Baseline để tính EOM cho các ngày còn lại

---

### 4. **Đếm số ngày còn lại theo date_label**
```sql
SELECT date_label, COUNT(*) as so_ngay
FROM dim_date
WHERE year = 2026 AND month = 1
  AND calendar_date >= '2026-01-06'  -- Từ ngày mai
  AND calendar_date <= '2026-01-31'  -- Đến cuối tháng
GROUP BY date_label
```
**Tác dụng:** Đếm số ngày còn lại từ ngày hiện tại đến cuối tháng, nhóm theo date_label

---

### 5. **Lấy uplift từ kpi_day_metadata**
```sql
SELECT date_label, uplift
FROM hskcdp.kpi_day_metadata
WHERE year = 2026 AND month = 1
```
**Tác dụng:** Lấy hệ số uplift (hệ số so với Normal day) cho từng date_label

---

### 6. **Lấy actual tháng (monthly actual)**
```sql
SELECT toMonth(t.created_at) as month,
       SUM(t.transaction_total) as monthly_actual
FROM hskcdp.object_sql_transactions AS t FINAL
WHERE toYear(t.created_at) = 2026
  AND t.status NOT IN ('Canceled', 'Cancel')
GROUP BY month
```
**Tác dụng:** Lấy actual revenue theo tháng (dự phòng nếu không có actual theo ngày)

EOM => 3456

---

## Công Thức Tính Toán

### EOM (End of Month)
```
EOM = Sum(actual) + Sum(rev eom)

Trong đó:
- Sum(actual) = Tổng actual từ transactions
- Sum(rev eom) = Σ(số_ngày_còn_lại × avg_normal_day × uplift)
```

### Gap Distribution
```
- Gap = EOM - kpi_initial
- Gap per remaining month = Total gap / Số tháng chưa có actual
- kpi_adjustment (tháng chưa có actual) = kpi_initial - gap_per_remaining_month
```

---

## Output

**Bảng:** `hskcdp.kpi_month`

**Các cột chính:**
- `kpi_initial`: Base KPI
- `actual_2026`: Actual revenue
- `eom`: End of Month
- `gap`: Gap = actual/eom - kpi_initial
- `kpi_adjustment`: KPI sau điều chỉnh
