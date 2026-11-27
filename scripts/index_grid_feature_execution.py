"""Publish an execution report to Elasticsearch with a stable document ID."""

import argparse
import hashlib
import json
from pathlib import Path

from elasticsearch import BadRequestError, Elasticsearch

from simulate_workloads import utc_timestamp


INDEX = "grid-feature-executions"
PROPERTIES = {
    **{field: {"type": "date"} for field in (
        "recorded_at", "fixed_start", "selected_start", "started_at", "deadline",
        "forecast_issued_at", "observation_date_utc",
    )},
    **{field: {"type": "keyword"} for field in (
        "run_id", "mode", "fallback_status", "grid_zone",
    )},
    **{field: {"type": "double"} for field in (
        "runtime_seconds", "estimated_emissions_saved_g", "coverage_fraction",
        "mean_gco2e_kwh",
    )},
    "observed_hours": {"type": "integer"},
    "completed_on_time": {"type": "boolean"},
}


def execution_document(report):
    plan = report["plan"]
    features = report["features"]
    return {
        "run_id": report["run_id"],
        "recorded_at": report["completed_at"],
        "fixed_start": plan["fixed_start"],
        "selected_start": plan["start"],
        "started_at": report["started_at"],
        "deadline": plan["deadline"],
        "forecast_issued_at": plan.get("forecast_issued_at"),
        "mode": plan["mode"],
        "fallback_status": plan["fallback_reason"] or "none",
        "runtime_seconds": report["runtime_seconds"],
        "estimated_emissions_saved_g": plan.get("estimated_emissions_saved_g"),
        "completed_on_time": utc_timestamp(report["completed_at"]) <= utc_timestamp(plan["deadline"]),
        "grid_zone": features["grid_zone"],
        "observation_date_utc": features["observation_date_utc"],
        "observed_hours": features["observed_hours"],
        "coverage_fraction": features["coverage_fraction"],
        "mean_gco2e_kwh": features["mean_gco2e_kwh"],
    }


def index_reports(paths):
    with Elasticsearch("http://elasticsearch:9200", request_timeout=30) as client:
        try:
            client.indices.create(
                index=INDEX, mappings={"properties": PROPERTIES},
                settings={"number_of_replicas": 0},
            )
        except BadRequestError as error:
            if error.body.get("error", {}).get("type") != "resource_already_exists_exception":
                raise
        for path in paths:
            report = json.loads(path.read_text(encoding="utf-8"))
            key = hashlib.sha256(report["run_id"].encode("utf-8")).hexdigest()
            client.index(index=INDEX, id=key, document=execution_document(report), refresh="wait_for")
            print(f"Indexed execution {report['run_id']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path, nargs="+")
    index_reports(parser.parse_args().reports)
