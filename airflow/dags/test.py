import os
import sys
from datetime import datetime

from airflow import DAG 
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

default_args = {"owner": "cdp-kpi-models", "retries": 1}

PYTHON_CMD = "PYTHONPATH=/opt/airflow python"

# kpi_day_metadata
with DAG(
    dag_id="kpi_day_metadata",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 25 * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models","monthly", "kpi_day_metadata"],
) as dag:
    kpi_day_metadata_task = BashOperator(
        task_id="kpi_day_metadata_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_day_metadata",
    )

# kpi_month
with DAG(
    dag_id="kpi_month",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models","daily", "kpi_month"],
) as dag:
    kpi_month_task = BashOperator(
        task_id="kpi_month_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_month",
    )

    trigger_kpi_day = TriggerDagRunOperator(
        task_id="trigger_kpi_day",
        trigger_dag_id="kpi_day",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    kpi_month_task >> trigger_kpi_day

# kpi_day
with DAG(
    dag_id="kpi_day",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models","hourly", "kpi_day"],
) as dag:
    kpi_day_task = BashOperator(
        task_id="kpi_day_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_day",
    )

# kpi_month_marketing_adjustment (Manual DAG - chạy khi marketing chỉnh sửa số)
with DAG(
    dag_id="kpi_month_marketing_adjustment",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "marketing-adjustment"],
    params={
        "version": "Thang 2",  # Version cần tính lại
        "month": 2,  # Tháng được marketing chỉnh sửa
        "new_kpi_initial": 10000000000.0  # Giá trị kpi_initial mới
    }
) as dag:
    recalculate_task = BashOperator(
        task_id="recalculate_version_after_marketing_adjustment",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_month "
                     "--recalculate-version '{{ params.version }}' "
                     "--month {{ params.month }} "
                     "--new-kpi-initial {{ params.new_kpi_initial }}",
    )