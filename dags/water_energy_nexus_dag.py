from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "khushi",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="water_energy_nexus",
    default_args=default_args,
    description="Water-Energy Nexus: AI Data Center Impact Tracker",
    schedule_interval="0 * * * *",  # every hour
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["big-data", "sustainability", "AI"],
) as dag:

    ingest_electricitymaps = BashOperator(
        task_id="ingest_electricitymaps",
        bash_command="python3 /opt/airflow/scripts/ingest_electricitymaps.py",
    )

    ingest_hubeau = BashOperator(
        task_id="ingest_hubeau",
        bash_command="python3 /opt/airflow/scripts/ingest_hubeau.py",
    )

    format_electricitymaps = BashOperator(
        task_id="format_electricitymaps",
        bash_command="docker exec big_data_project-spark-1 /opt/spark/bin/spark-submit /scripts/format_electricitymaps.py",
    )

    format_hubeau = BashOperator(
        task_id="format_hubeau",
        bash_command="docker exec big_data_project-spark-1 /opt/spark/bin/spark-submit /scripts/format_hubeau.py",
    )

    combine = BashOperator(
        task_id="combine_stress_score",
        bash_command="docker exec big_data_project-spark-1 /opt/spark/bin/spark-submit /scripts/combine.py",
    )

    index_elasticsearch = BashOperator(
        task_id="index_elasticsearch",
        bash_command="docker exec big_data_project-spark-1 /opt/spark/bin/spark-submit /scripts/index_elasticsearch.py",
    )
    
    # Pipeline order
    ingest_electricitymaps >> ingest_hubeau >> format_electricitymaps >> format_hubeau >> combine >> index_elasticsearch