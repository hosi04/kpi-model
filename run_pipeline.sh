#!/bin/bash

# Script để chạy toàn bộ KPI pipeline
# Sử dụng: ./run_pipeline.sh hoặc bash run_pipeline.sh

echo "============================================================"
echo "BAT DAU CHAY KPI PIPELINE"
echo "============================================================"

# Danh sách các bước
steps=(
    "src.etl.kpi_day_metadata:Tính toán KPI Day Metadata"
    "src.etl.kpi_month:Tính toán KPI Month"
    "src.etl.kpi_day:Tính toán KPI Day"
    "src.etl.kpi_channel_metadata:Tính toán KPI Channel Metadata"
    "src.etl.kpi_channel:Tính toán KPI Channel"
    "src.etl.kpi_brand_metadata:Tính toán KPI Brand Metadata"
    "src.etl.kpi_brand:Tính toán KPI Brand"
)

failed_steps=()

for step in "${steps[@]}"; do
    IFS=':' read -r module description <<< "$step"
    
    echo ""
    echo "============================================================"
    echo "$description"
    echo "============================================================"
    echo "Chạy: python -m $module"
    
    start_time=$(date +%s)
    
    if python -m "$module"; then
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo "[OK] Hoàn thành $description (Thời gian: ${duration}s)"
    else
        echo "[ERROR] Lỗi khi chạy $description"
        failed_steps+=("$description")
        echo ""
        read -p "Bạn có muốn tiếp tục với các bước tiếp theo? (y/n): " continue_choice
        if [ "$continue_choice" != "y" ]; then
            break
        fi
    fi
done

echo ""
echo "============================================================"
echo "KET THUC PIPELINE"
echo "============================================================"

if [ ${#failed_steps[@]} -eq 0 ]; then
    echo "[OK] Tất cả các bước đã hoàn thành thành công!"
    exit 0
else
    echo "[ERROR] Các bước bị lỗi:"
    for step in "${failed_steps[@]}"; do
        echo "   - $step"
    done
    exit 1
fi
