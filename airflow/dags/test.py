import os
import sys
from datetime import datetime

from airflow import DAG 
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

default_args = {"owner": "cdp-kpi-models", "retries": 1}

PYTHON_CMD = "PYTHONPATH=/opt/airflow python"

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
    schedule="30 0 * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models","daily", "kpi_month"],
) as dag:
    kpi_month_task = BashOperator(
        task_id="kpi_month_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_month",
    )

# Or pass via conf when triggering: {"kpi_month_target_month": "1"}
with DAG(
    dag_id="kpi_month_recalculate_version",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_month", "recalculate"],
) as dag:
    kpi_month_recalculate_task = BashOperator(
        task_id="kpi_month_recalculate_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_month --target-month {{{{ var.value.get('kpi_month_target_month', '1') }}}}",
    )

# Or pass via conf when triggering: {"kpi_month_source_month": "1"}
with DAG(
    dag_id="kpi_month_create_version_manually",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_month", "create_version"],
) as dag:
    kpi_month_create_version_task = BashOperator(
        task_id="kpi_month_create_version_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_month --create-version-manually --source-month {{{{ var.value.get('kpi_month_source_month', '1') }}}} --force",
    )


# Or pass via conf when triggering: {"kpi_day_metadata_target_month": "2"}
with DAG(
    dag_id="kpi_day_metadata_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_day_metadata"],
) as dag:
    kpi_day_metadata_manual_task = BashOperator(
        task_id="kpi_day_metadata_manual_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_day_metadata --target-month {{{{ dag_run.conf.get('kpi_day_metadata_target_month', '') }}}}",
    )

# KPI BRAND
with DAG(
    dag_id="kpi_brand",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "daily", "kpi_brand"],
) as dag:
    kpi_brand_task = BashOperator(
        task_id="kpi_brand_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_brand",
    )

# Or pass via conf when triggering: {"kpi_brand_target_month": "2"}
with DAG(
    dag_id="kpi_brand_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_brand"],
) as dag:
    kpi_brand_manual_task = BashOperator(
        task_id="kpi_brand_manual_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_brand --target-month {{{{ dag_run.conf.get('kpi_brand_target_month', '') }}}}",
    )

# KPI BRAND METADATA (schedule default hàng tháng)
with DAG(
    dag_id="kpi_brand_metadata",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 1 * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "monthly", "kpi_brand_metadata"],
) as dag:
    kpi_brand_metadata_task = BashOperator(
        task_id="kpi_brand_metadata_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_brand_metadata",
    )

# Or pass via conf when triggering: {"kpi_brand_metadata_target_month": "2"}
with DAG(
    dag_id="kpi_brand_metadata_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_brand_metadata"],
) as dag:
    kpi_brand_metadata_manual_task = BashOperator(
        task_id="kpi_brand_metadata_manual_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_brand_metadata --target-month {{{{ dag_run.conf.get('kpi_brand_metadata_target_month', '') }}}}",
    )

# KPI SKU (schedule default hàng ngày)
with DAG(
    dag_id="kpi_sku",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "daily", "kpi_sku"],
) as dag:
    kpi_sku_task = BashOperator(
        task_id="kpi_sku_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_sku",
    )

# Or pass via conf when triggering: {"kpi_sku_target_month": "2"}
with DAG(
    dag_id="kpi_sku_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_sku"],
) as dag:
    kpi_sku_manual_task = BashOperator(
        task_id="kpi_sku_manual_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_sku --target-month {{{{ dag_run.conf.get('kpi_sku_target_month', '') }}}}",
    )

# KPI FORECAST (schedule default hàng ngày)
with DAG(
    dag_id="kpi_forecast",
    start_date=datetime(2026, 1, 1),
    schedule="30 2 * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "daily", "kpi_forecast"],
) as dag:
    kpi_forecast_task = BashOperator(
        task_id="kpi_forecast_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_forecast",
    )

# Or pass via conf when triggering: {"kpi_forecast_target_month": "2", "kpi_forecast_target_year": "2026"}
with DAG(
    dag_id="kpi_forecast_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_forecast"],
) as dag:
    kpi_forecast_manual_task = BashOperator(
        task_id="kpi_forecast_manual_task",
        bash_command=(
            f"{PYTHON_CMD} -m src.etl.kpi_forecast "
            f"--target-month {{{{ dag_run.conf.get('kpi_forecast_target_month', '') }}}} "
            f"--target-year {{{{ dag_run.conf.get('kpi_forecast_target_year', '') }}}}"
        ),
    )
