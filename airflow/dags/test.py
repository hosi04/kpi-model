import os
import sys
import pendulum
from datetime import datetime

from airflow import DAG 
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

default_args = {"owner": "cdp-kpi-models", "retries": 1}

PYTHON_CMD = "PYTHONPATH=/opt/airflow python"

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# kpi_day_metadata
with DAG(
    dag_id="kpi_day_metadata",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 25 * *",
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
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule="30 0 * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models","daily", "kpi_month"],
) as dag:
    kpi_month_task = BashOperator(
        task_id="kpi_month_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_month",
    )

# Or pass via conf when triggering: {"kpi_month_target_month": "2"}
with DAG(
    dag_id="kpi_month_recalculate_version",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_month", "recalculate"],
) as dag:
    kpi_month_recalculate_task = BashOperator(
        task_id="kpi_month_recalculate_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_month --target-month {{{{ dag_run.conf.get('kpi_month_target_month', '') }}}}",
    )

# Or pass via conf when triggering: {"kpi_month_source_month": "1"}
with DAG(
    dag_id="kpi_month_create_version_manually",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_month", "create_version"],
) as dag:
    kpi_month_create_version_task = BashOperator(
        task_id="kpi_month_create_version_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_month --create-version-manually --source-month {{{{ dag_run.conf.get('kpi_month_source_month', '') }}}} --force",
    )

# Or pass via conf when triggering:
# {
#   "kpi_month_recalc_version": "Thang 2",
#   "kpi_month_recalc_month": "2",
#   "kpi_month_new_kpi_initial": "300"
# }
with DAG(
    dag_id="kpi_month_recalculate_after_marketing",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_month", "recalculate_after_marketing"],
) as dag:
    kpi_month_recalculate_after_marketing_task = BashOperator(
        task_id="kpi_month_recalculate_after_marketing_task",
        bash_command=(
            f'{PYTHON_CMD} -m src.etl.kpi_month '
            f'--recalculate-version "{{{{ dag_run.conf.get(\'kpi_month_recalc_version\', \'\') }}}}" '
            f'--month {{{{ dag_run.conf.get("kpi_month_recalc_month", "") }}}} '
            f'--new-kpi-initial {{{{ dag_run.conf.get("kpi_month_new_kpi_initial", "") }}}}'
        ),
    )

    # trigger_kpi_month_after_recalc = TriggerDagRunOperator(
    #     task_id="trigger_kpi_month_after_recalc",
    #     trigger_dag_id="kpi_month",
    #     wait_for_completion=False,
    #     reset_dag_run=True,
    # )

    # kpi_month_recalculate_after_marketing_task >> trigger_kpi_month_after_recalc

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

# kpi_day - schedule hourly
with DAG(
    dag_id="kpi_day",
    start_date=datetime(2026, 1, 1),
    schedule="25 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "hourly", "kpi_day"],
) as dag:
    kpi_day_task = BashOperator(
        task_id="kpi_day_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_day",
    )

# Or pass via conf when triggering: {"kpi_day_target_month": "2"}
with DAG(
    dag_id="kpi_day_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_day"],
) as dag:
    kpi_day_manual_task = BashOperator(
        task_id="kpi_day_manual_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_day --target-month {{{{ dag_run.conf.get('kpi_day_target_month', '') }}}}",
    )

# kpi_channel_metadata
with DAG(
    dag_id="kpi_channel_metadata",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 25 * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models","monthly", "kpi_channel_metadata"],
) as dag:
    kpi_channel_metadata_task = BashOperator(
        task_id="kpi_channel_metadata_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_channel_metadata",
    )

# Or pass via conf when triggering: {"kpi_channel_metadata_target_month": "2"}
with DAG(
    dag_id="kpi_channel_metadata_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_channel_metadata"],
) as dag:
    kpi_channel_metadata_manual_task = BashOperator(
        task_id="kpi_channel_metadata_manual_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_channel_metadata --target-month {{{{ dag_run.conf.get('kpi_channel_metadata_target_month', '') }}}}",
    )

# kpi_channel - triggered by kpi_day
with DAG(
    dag_id="kpi_channel",
    start_date=datetime(2026, 1, 1),
    schedule="35 * * * *",  # Also scheduled hourly, but triggered by kpi_day
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "hourly", "kpi_channel"],
) as dag:
    kpi_channel_task = BashOperator(
        task_id="kpi_channel_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_channel",
    )

# Or pass via conf when triggering: {"kpi_channel_target_month": "2"}
with DAG(
    dag_id="kpi_channel_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_channel"],
) as dag:
    kpi_channel_manual_task = BashOperator(
        task_id="kpi_channel_manual_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_channel --target-month {{{{ dag_run.conf.get('kpi_channel_target_month', '') }}}}",
    )

# KPI BRAND
with DAG(
    dag_id="kpi_brand",
    start_date=datetime(2026, 1, 1),
    schedule="40 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "hourly", "kpi_brand"],
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

# KPI BRAND METADATA
with DAG(
    dag_id="kpi_brand_metadata",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 25 * *",
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

# KPI SKU
with DAG(
    dag_id="kpi_sku",
    start_date=datetime(2026, 1, 1),
    schedule="45 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "hourly", "kpi_sku"],
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

# KPI FORECAST
with DAG(
    dag_id="kpi_forecast",
    start_date=datetime(2026, 1, 1),
    schedule="20 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "hourly", "kpi_forecast"],
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

# KPI SKU METADATA
with DAG(
    dag_id="kpi_sku_metadata",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 25 * *",
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "monthly", "kpi_sku_metadata"],
) as dag:
    kpi_sku_metadata_task = BashOperator(
        task_id="kpi_sku_metadata_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_sku_metadata",
    )

# Or pass via conf when triggering: {"kpi_sku_metadata_target_month": "2"}
with DAG(
    dag_id="kpi_sku_metadata_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["cdp-kpi-models", "manual", "kpi_sku_metadata"],
) as dag:
    kpi_sku_metadata_manual_task = BashOperator(
        task_id="kpi_sku_metadata_manual_task",
        bash_command=f"{PYTHON_CMD} -m src.etl.kpi_sku_metadata --target-month {{{{ dag_run.conf.get('kpi_sku_metadata_target_month', '') }}}}",
    )
