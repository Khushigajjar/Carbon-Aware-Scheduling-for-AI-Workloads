from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="workload_scheduling_shadow",
    description="Record carbon-aware batch scheduling recommendations",
    start_date=datetime(2026, 11, 21, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["sustainability", "shadow-mode"],
) as dag:
    record_recommendations = BashOperator(
        task_id="record_recommendations",
        bash_command="python3 /opt/airflow/scripts/record_shadow_decisions.py",
        env={
            "SHADOW_RUN_ID": "{{ run_id }}",
            "SHADOW_SCENARIO": "{{ dag_run.conf | tojson }}",
            "SHADOW_OUTPUT_DIR": "/opt/airflow/data/usage/workload_shadow",
        },
        append_env=True,
        do_xcom_push=False,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    index_recommendations = BashOperator(
        task_id="index_recommendations",
        bash_command="python3 /opt/airflow/scripts/index_shadow_decisions.py",
        env={
            "SHADOW_RUN_ID": "{{ run_id }}",
            "SHADOW_RECORDED_AT": "{{ logical_date.isoformat() }}",
            "SHADOW_OUTPUT_DIR": "/opt/airflow/data/usage/workload_shadow",
        },
        append_env=True,
        do_xcom_push=False,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    record_recommendations >> index_recommendations
