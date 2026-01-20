# Pipeline Slides Summary - Chi Tiết Từng File

## 1. KPI DAY METADATA (@monthly)

### Mục đích
Tính toán metadata (uplift, weight) cho các date_label dựa trên historical data 3 tháng gần nhất. Metadata này được dùng để phân bổ KPI từ tháng xuống ngày.

### Cách làm
1. **Xác định 3 tháng gần nhất** trước target_month
   - Ví dụ: Tháng 1/2026 → lấy data từ 10, 11, 12/2025
   - Tháng 2/2026 → lấy data từ 11, 12/2025 và 1/2026

2. **Lấy historical revenue** theo date_label từ transactions (3 tháng gần nhất)
   - Tính `avg_total` (trung bình doanh thu/ngày) cho từng date_label
   - Baseline = `avg_total` của "Normal day"

3. **Tính uplift** cho từng date_label
   - `uplift = avg_total / baseline`
   - Hệ số so với Normal day

4. **Đếm số ngày** theo date_label trong tháng target

5. **Tính weight** = số_ngày × uplift

### Query chính
- **Historical revenue by date_label**: Lấy avg_total từ transactions (3 tháng gần nhất)
- **Đếm số ngày theo date_label**: Từ dim_date

### Output
Bảng `kpi_day_metadata`: uplift, weight, avg_total cho từng date_label

---

## 2. KPI MONTH (@hourly)

### Mục đích
Tính toán `kpi_adjustment` cho các tháng, bao gồm EOM (End of Month) và phân bổ gap cho các tháng chưa có actual.

### Cách làm
1. **Lấy kpi_initial** (base KPI) từ `kpi_month_base`

2. **Tính EOM** cho các tháng có actual theo ngày:
   - `EOM = Sum(actual) + Sum(rev eom)`
   - `Sum(rev eom) = Σ(số_ngày_còn_lại × avg_normal_day × uplift)`

3. **Tính gap**:
   - Tháng có EOM: `gap = EOM - kpi_initial`
   - Tháng có actual tháng: `gap = actual - kpi_initial`

4. **Phân bổ gap** cho các tháng chưa có actual:
   - `gap_per_month = Total gap / Số tháng chưa có actual`
   - `kpi_adjustment = kpi_initial - gap_per_month`

### Query chính
- **Lấy kpi_initial**: Từ `kpi_month_base`
- **Lấy Sum(actual)**: Từ transactions theo ngày
- **Lấy avg_normal_day**: 30 ngày gần nhất
- **Đếm số ngày còn lại**: Theo date_label từ dim_date
- **Lấy uplift**: Từ `kpi_day_metadata`
- **Lấy actual tháng**: Từ transactions (dự phòng)

### Output
Bảng `kpi_month`: kpi_initial, actual_2026, eom, gap, kpi_adjustment

---

## 3. KPI DAY (@hourly)

### Mục đích
Tính toán `kpi_day_initial`, `kpi_day_adjustment`, và `eod` (End of Day) cho các ngày trong tháng.

### Cách làm

#### A. Tính kpi_day_initial
- Lấy `kpi_month` (kpi_initial) từ `kpi_month`
- Lấy `uplift`, `weight` từ `kpi_day_metadata`
- `kpi_day_initial = (uplift × kpi_month) / total_weight_month`

#### B. Tính kpi_day_adjustment
1. **Lấy actual** theo ngày từ transactions

2. **Tính gap**:
   - Ngày có actual: `gap = actual - kpi_day_initial`
   - Ngày đã qua không có actual: `gap = -kpi_day_initial`
   - Ngày chưa qua không có actual: `gap = None`

3. **Phân bổ gap** cho ngày chưa có actual:
   - `weighted_left = uplift` (cho ngày chưa qua)
   - `kpi_day_adjustment = kpi_day_initial - (SUM(Gap) × uplift / SUM(weighted_left))`

#### C. Tính EOD (cho ngày hiện tại)
- Lấy % doanh thu theo giờ (30 ngày gần nhất)
- Lấy actual từ 0h00 đến <giờ hiện tại>
- `EOD = actual_until_hour / sum(% của các giờ đã qua)`

### Query chính
- **Lấy kpi_month và metadata**: Join `kpi_month` + `kpi_day_metadata` + `dim_date`
- **Lấy actual theo ngày**: Từ transactions
- **Lấy % doanh thu theo giờ**: 30 ngày gần nhất (cho EOD)
- **Lấy actual từ 0h đến giờ hiện tại**: Cho EOD

### Output
Bảng `kpi_day`: kpi_day_initial, actual, gap, kpi_day_adjustment, weighted_left, eod

---

## Tóm tắt Dependencies

```
month_base.py (one-time)
    ↓
kpi_day_metadata.py (@monthly)
    ↓
    ├─→ kpi_month.py (@hourly) ──┐
    │                           │
    └─→ kpi_day.py (@hourly) ←───┘
         (cần kpi_month.kpi_initial)
```

**Lưu ý:**
- `kpi_day_metadata` chạy monthly vì cần cập nhật 3 tháng gần nhất
- `kpi_month` và `kpi_day` chạy hourly để cập nhật real-time
- `kpi_day` phụ thuộc vào `kpi_month` (cần kpi_initial)
