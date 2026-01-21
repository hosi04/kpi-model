import os
import sys
from datetime import datetime

from airflow import DAG 
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

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