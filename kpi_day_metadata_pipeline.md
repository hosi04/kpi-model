# Pipeline Tính Toán KPI Day Metadata (Slide Version)

## Tổng quan
File `kpi_day_metadata.py` tính toán metadata (uplift, weight) cho các date_label dựa trên historical data 3 tháng gần nhất.

## Pipeline Flow (Tóm tắt)

```
1. Xác định 3 tháng gần nhất trước target_month
   ↓
2. Lấy historical revenue theo date_label (3 tháng gần nhất)
   ├─→ Tính avg_total (trung bình doanh thu/ngày) cho từng date_label
   └─→ Baseline = avg_total của "Normal day"
   ↓
3. Tính uplift cho từng date_label
   └─→ uplift = avg_total / baseline
   ↓
4. Đếm số ngày theo date_label trong tháng target
   ↓
5. Tính weight = số_ngày × uplift
   ↓
6. Lưu kết quả vào kpi_day_metadata
```

---

## Các Main Query Chính

### 1. **Lấy historical revenue theo date_label (3 tháng gần nhất)**
```sql
SELECT 
    a.date_label,
    AVG(a.daily_revenue) as avg_total,
    COUNT(DISTINCT a.calendar_date) as so_ngay_historical
FROM (
    SELECT 
        d.calendar_date,
        d.date_label,
        SUM(t.transaction_total) as daily_revenue
    FROM hskcdp.object_sql_transactions AS t FINAL
    INNER JOIN hskcdp.dim_date d
        ON toDate(t.created_at) = d.calendar_date
    WHERE toDate(t.created_at) >= today() - INTERVAL 3 MONTH
      AND d.date_label IN ('Normal day', 'Pay Day', ...)
      AND t.status NOT IN ('Cancel', 'Canceled')
    GROUP BY d.calendar_date, d.date_label
) as a
GROUP BY a.date_label
```
**Tác dụng:** Tính trung bình doanh thu/ngày cho từng date_label trong 3 tháng gần nhất từ transactions

---

### 2. **Đếm số ngày theo date_label trong tháng target**
```sql
SELECT 
    date_label,
    COUNT(*) as so_ngay
FROM dim_date
WHERE year = 2026
  AND month = 1
  AND date_label IN ('Normal day', 'Pay Day', ...)
  AND NOT (
      (month = 6 AND day = 6) OR
      (month = 9 AND day = 9) OR
      (month = 11 AND day = 11) OR
      (month = 12 AND day = 12)
  )
GROUP BY date_label
```
**Tác dụng:** Đếm số ngày của từng date_label trong tháng đang tính (để tính weight)

---

## Công Thức Tính Toán

### Uplift
```
uplift = avg_total / baseline

Trong đó:
- avg_total = Trung bình doanh thu/ngày của date_label (3 tháng gần nhất)
- baseline = avg_total của "Normal day"
```

### Weight
```
weight = số_ngày × uplift

Trong đó:
- số_ngày = Số ngày của date_label trong tháng target
- uplift = Hệ số so với Normal day
```

---

## Output

**Bảng:** `hskcdp.kpi_day_metadata`

**Các cột chính:**
- `year`, `month`: Năm và tháng đang tính
- `date_label`: Loại ngày (Normal day, Pay Day, Double Day, ...)
- `avg_total`: Trung bình doanh thu/ngày (3 tháng gần nhất)
- `uplift`: Hệ số so với Normal day
- `so_ngay`: Số ngày của date_label trong tháng
- `weight`: Weight = số_ngày × uplift
- `total_weight_month`: Tổng weight của cả tháng
