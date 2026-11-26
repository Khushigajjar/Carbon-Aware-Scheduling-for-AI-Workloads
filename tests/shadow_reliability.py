"""Exercise shadow scheduling against the local Airflow and Elasticsearch stack."""

import argparse
import copy
import getpass
import hashlib
import http.cookiejar
import json
import re
import subprocess
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen

ROOT = Path(__file__).resolve().parents[1]
AIRFLOW = "http://localhost:8080"
DAG = "/api/v1/dags/workload_scheduling_shadow"
ES = "http://localhost:9200/workload-shadow-decisions"


def request(opener, url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    with opener(Request(url, data, {"Content-Type": "application/json"}), timeout=15) as response:
        return json.load(response)


def wait_for_success(opener, run_id):
    path = AIRFLOW + DAG + "/dagRuns/" + run_id
    for _ in range(60):
        run = request(opener, path)
        if run["state"] == "success":
            return run
        if run["state"] == "failed":
            raise RuntimeError(f"Airflow run failed: {run_id}")
        time.sleep(3)
    raise TimeoutError(run_id)


def indexed_jobs(run_id):
    response = request(urlopen, ES + "/_search", {"query": {"term": {"run_id": run_id}}})
    return {hit["_id"]: hit["_source"] for hit in response["hits"]["hits"]}


def test_outage(opener, docker, run_id, scenario):
    compose = [str(docker), "compose"]
    active = subprocess.check_output(
        compose + ["ps", "--status", "running", "--services", "elasticsearch"],
        cwd=ROOT, text=True,
    )
    if "elasticsearch" not in active.splitlines():
        raise RuntimeError("Start Elasticsearch before the outage test")
    try:
        subprocess.run(compose + ["stop", "elasticsearch"], cwd=ROOT, check=True)
        request(opener, AIRFLOW + DAG + "/dagRuns", {"dag_run_id": run_id, "conf": scenario})
        run_path = AIRFLOW + DAG + "/dagRuns/" + run_id
        task_path = run_path + "/taskInstances/index_recommendations"
        for _ in range(35):
            task = request(opener, task_path)
            if task["state"] in ("up_for_retry", "failed"):
                break
            time.sleep(2)
        else:
            raise TimeoutError("Indexing never reported an outage failure")
        filename = hashlib.sha256(run_id.encode()).hexdigest() + ".json"
        report = ROOT / "data/usage/workload_shadow" / filename
        assert json.loads(report.read_bytes())["run_id"] == run_id
        print(f"Verified outage state {task['state']} and preserved report", flush=True)
    finally:
        subprocess.run(compose + ["start", "elasticsearch"], cwd=ROOT, check=True)
    if task["state"] == "failed":
        request(opener, AIRFLOW + DAG + "/clearTaskInstances", {
            "dry_run": False, "dag_run_id": run_id, "task_ids": ["index_recommendations"],
            "only_failed": True, "reset_dag_runs": True,
        })
    wait_for_success(opener, run_id)
    jobs = indexed_jobs(run_id)
    assert len(jobs) == len(scenario["jobs"]), jobs
    return {"run_id": run_id, "outage_task_state": task["state"],
            "preserved_report": filename, "recovered_document_ids": list(jobs)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--username", default="admin")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--docker", type=Path, help="Docker executable for the outage test")
    args = parser.parse_args()
    opener = build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))
    login_url = AIRFLOW + "/login/"
    with opener.open(login_url, timeout=15) as response:
        token = re.search(r'name="csrf_token"[^>]*value="([^"]+)', response.read().decode())[1]
    form = urlencode({"username": args.username, "password": getpass.getpass("Airflow password: "),
                      "csrf_token": token}).encode()
    with opener.open(login_url, form, timeout=15):
        pass

    sample = json.loads((ROOT / "examples/workload_scenario.json").read_text())
    sample["data_origin"] = "reliability_test"
    sample["jobs"] = sample["jobs"][:1]
    missing = dict(sample, forecast=[])
    stale = dict(sample, forecast_issued_at="2026-11-21T17:00:00Z")
    fresh = dict(sample, forecast_issued_at="2026-11-21T23:00:00Z")
    impossible = copy.deepcopy(sample)
    impossible["jobs"][0]["runtime_minutes"] = 361
    cases = {
        "missing": (missing, "insufficient_forecast_coverage", True),
        "impossible": (impossible, "runtime_exceeds_available_time", False),
        "stale": (stale, "stale_forecast", True),
        "fresh": (fresh, None, True),
    }
    results = {}
    suffix = uuid.uuid4().hex[:10]
    run_ids = {}
    for name, (scenario, _, _) in cases.items():
        run_id = f"reliability_{name}_{suffix}"
        request(opener.open, AIRFLOW + DAG + "/dagRuns",
                {"dag_run_id": run_id, "conf": scenario})
        run_ids[name] = run_id
        print(f"Triggered {run_id}", flush=True)

    for name, (_, expected_reason, has_start) in cases.items():
        run_id = run_ids[name]
        wait_for_success(opener.open, run_id)
        jobs = indexed_jobs(run_id)
        decision, = jobs.values()
        assert decision["fallback_reason"] == expected_reason, decision
        assert bool(decision["recommended_start"]) == has_start, decision
        if expected_reason:
            assert decision["estimated_emissions_saved_g"] is None, decision
            if has_start:
                assert decision["fixed_start"] == decision["recommended_start"], decision
        else:
            assert decision["estimated_emissions_saved_g"] == 2140, decision
        results[name] = {"run_id": run_id, "decision": decision}
        print(f"Passed {name}", flush=True)

    run_id = run_ids["fresh"]
    before = indexed_jobs(run_id)
    request(opener.open, AIRFLOW + DAG + "/clearTaskInstances", {
        "dry_run": False, "dag_run_id": run_id, "task_ids": ["index_recommendations"],
        "only_failed": False, "only_running": False, "reset_dag_runs": True,
    })
    wait_for_success(opener.open, run_id)
    after = indexed_jobs(run_id)
    assert before == after, after
    results["retry"] = {"run_id": run_id, "document_ids": list(after), "passed": True}
    args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    if args.docker:
        results["outage"] = test_outage(
            opener.open, args.docker, f"reliability_outage_{suffix}", fresh
        )
        args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(f"Saved {args.output}", flush=True)


if __name__ == "__main__":
    main()
