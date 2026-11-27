import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.sensors.date_time import DateTimeSensor


SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


@dag(
    dag_id="daily_grid_features_production",
    start_date=datetime(2026, 11, 27, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["production-pilot", "ml-features"],
)
def daily_grid_features():
    @task(multiple_outputs=True)
    def plan():
        sys.path.insert(0, str(SCRIPTS_DIR))
        from grid_feature_workload import plan_run

        context = get_current_context()
        return plan_run(context["dag_run"].conf, datetime.now(timezone.utc))

    @task(execution_timeout=timedelta(minutes=10))
    def build(selected_plan):
        sys.path.insert(0, str(SCRIPTS_DIR))
        from grid_feature_workload import run_workload

        context = get_current_context()
        output = run_workload(Path("/opt/airflow/data"), context["run_id"], selected_plan)
        print(f"Published daily grid features to {output}")
        return str(output)

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def index_execution(report_path):
        sys.path.insert(0, str(SCRIPTS_DIR))
        from index_grid_feature_execution import index_reports

        index_reports([Path(report_path)])

    selected = plan()
    wait = DateTimeSensor(
        task_id="wait_for_selected_start",
        target_time="{{ ti.xcom_pull(task_ids='plan', key='start') }}",
        mode="reschedule",
        poke_interval=15,
        timeout=24 * 60 * 60,
    )
    report = build(selected)
    selected >> wait >> report
    index_execution(report)


daily_grid_features()
