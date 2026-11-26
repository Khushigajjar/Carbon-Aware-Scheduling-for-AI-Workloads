"""Index the saved recommendations for one Airflow run."""

import json
import os
from pathlib import Path

from elasticsearch import BadRequestError, Elasticsearch, helpers

from record_shadow_decisions import report_path


INDEX = "workload-shadow-decisions"
PROPERTIES = {
    "recorded_at": {"type": "date"},
    **{field: {"type": "keyword"} for field in (
        "run_id", "job_name", "data_origin", "fallback_reason", "deadline_status"
    )},
    **{field: {"type": "date"} for field in (
        "fixed_start", "recommended_start", "recommended_end"
    )},
    "estimated_emissions_saved_g": {"type": "double"},
    "estimated_electricity_cost_saved_eur": {"type": "double"},
    "used_fallback": {"type": "boolean"},
}


def documents(report, recorded_at):
    for position, decision in enumerate(report["decisions"]):
        recommended = decision["recommended_schedule"]
        fallback = decision["fallback_reason"]
        yield {
            "_index": INDEX,
            "_id": f'{report["run_id"]}:{position}',
            "_source": {
                "recorded_at": recorded_at,
                "run_id": report["run_id"],
                "job_name": decision["job_name"],
                "data_origin": report.get("data_origin", "unknown"),
                "fixed_start": decision["fixed_schedule"]["start"],
                "recommended_start": recommended["start"] if recommended else None,
                "recommended_end": recommended["end"] if recommended else None,
                "deadline_status": (
                    "no_feasible_window" if not recommended
                    else "projected_on_time" if recommended["meets_deadline"]
                    else "projected_late"
                ),
                "used_fallback": fallback is not None,
                "fallback_reason": fallback,
                "estimated_emissions_saved_g": decision["estimated_emissions_saved_g"],
                "estimated_electricity_cost_saved_eur": decision[
                    "estimated_electricity_cost_saved_eur"
                ],
            },
        }


def main():
    run_id = os.environ["SHADOW_RUN_ID"]
    output_dir = Path(os.environ["SHADOW_OUTPUT_DIR"])
    report = json.loads(report_path(run_id, output_dir).read_text(encoding="utf-8"))
    with Elasticsearch("http://elasticsearch:9200", request_timeout=30) as client:
        try:
            client.indices.create(
                index=INDEX,
                mappings={"properties": PROPERTIES},
                settings={"number_of_replicas": 0},
            )
        except BadRequestError as error:
            if error.body.get("error", {}).get("type") != "resource_already_exists_exception":
                raise
        recorded_at = report.get("recorded_at") or os.environ["SHADOW_RECORDED_AT"]
        count, _ = helpers.bulk(client, documents(report, recorded_at), refresh="wait_for")
    print(f"Indexed {count} decisions into {INDEX}")


if __name__ == "__main__":
    main()
