"""Persist one workload recommendation report per Airflow DAG run."""

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

from live_carbon_forecast import fetch_live_scenario
from simulate_workloads import simulate


def report_path(run_id, output_dir):
    return output_dir / (hashlib.sha256(run_id.encode("utf-8")).hexdigest() + ".json")


def record_decisions(run_id, scenario, output_dir):
    report = {
        "mode": "shadow",
        "run_id": run_id,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "data_origin": scenario.get("data_origin", "unknown"),
        "decisions": simulate(scenario),
    }
    if scenario.get("forecast_source"):
        report["forecast_metadata"] = {
            key: scenario[key] for key in (
                "forecast_source", "forecast_zone", "forecast_issued_at", "decision_at"
            )
        }
    content = json.dumps(report, indent=2, default=datetime.isoformat, allow_nan=False)
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = report_path(run_id, output_dir)
    # Write beside the destination so replacement is atomic on the same volume.
    with NamedTemporaryFile(dir=output_dir, delete=False) as temporary_file:
        temporary_path = Path(temporary_file.name)
    try:
        temporary_path.write_text(content + "\n", encoding="utf-8")
        temporary_path.replace(destination)
    finally:
        temporary_path.unlink(missing_ok=True)
    return destination


def main():
    try:
        run_id = os.environ["SHADOW_RUN_ID"]
        scenario = json.loads(os.environ["SHADOW_SCENARIO"])
        output_dir = Path(os.environ["SHADOW_OUTPUT_DIR"])
        if not isinstance(scenario, dict) or not scenario.get("jobs"):
            raise ValueError("Provide a scenario containing jobs and hourly forecasts")
        if scenario.get("forecast_source") == "electricity_maps_v5":
            destination = report_path(run_id, output_dir)
            if destination.exists():
                print(f"Reusing recorded shadow recommendations in {destination}")
                return
            scenario = fetch_live_scenario(scenario)
        destination = record_decisions(run_id, scenario, output_dir)
    except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
        raise SystemExit(f"Could not record shadow recommendations: {error}") from error
    print(f"Recorded shadow recommendations in {destination}")


if __name__ == "__main__":
    main()
