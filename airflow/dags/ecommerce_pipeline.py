from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ecommerce_gold_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["ecommerce", "analytics"],
) as dag:
    quality = BashOperator(
        task_id="validate_gold",
        bash_command="cd /workspace && python quality/validate_gold.py",
    )

    load = BashOperator(
        task_id="load_gold_to_warehouse",
        bash_command="cd /workspace && python jobs/load_gold_to_warehouse.py",
    )

    quality >> load
