"""Plan and execute a daily batch feature build from real grid observations."""

import argparse
import hashlib
import json
import math
import statistics
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import perf_counter

from live_carbon_forecast import fetch_live_scenario
from simulate_workloads import simulate, utc_timestamp


def plan_run(config, now):
    data_date = date.fromisoformat(config["data_date"]).isoformat()
    fixed_start = utc_timestamp(config["fixed_start"]) if "fixed_start" in config else now
    deadline = utc_timestamp(config["deadline"]) if "deadline" in config else now + timedelta(minutes=15)
    runtime = config.get("runtime_minutes", 1)
    if type(runtime) is not int or runtime < 1:
        raise ValueError("runtime_minutes must be a positive integer")
    latest_start = deadline - timedelta(minutes=runtime)
    if not now <= fixed_start <= latest_start or deadline > now + timedelta(hours=24):
        raise ValueError("Fixed start must fit the runtime within a deadline in the next 24 hours")

    plan = {
        "data_date": data_date,
        "start": fixed_start.isoformat(),
        "fixed_start": fixed_start.isoformat(),
        "deadline": deadline.isoformat(),
        "latest_start": latest_start.isoformat(),
        "mode": "fixed",
        "fallback_reason": None,
    }
    if config.get("mode", "fixed") == "fixed":
        return plan
    if config["mode"] != "carbon_aware":
        raise ValueError("mode must be fixed or carbon_aware")
    energy = config["energy_kwh"]
    if not math.isfinite(energy) or energy <= 0:
        raise ValueError("energy_kwh must be finite and positive")
    job = {
        "name": "daily-grid-features",
        "runtime_minutes": runtime,
        "energy_kwh": energy,
        "submitted_at": now.isoformat(),
        "fixed_start": fixed_start.isoformat(),
        "deadline": deadline.isoformat(),
    }
    try:
        scenario = fetch_live_scenario({"jobs": [job]})
        decision, = simulate(scenario)
    except (KeyError, TypeError, ValueError, OSError) as error:
        plan["fallback_reason"] = "live_forecast_unavailable"
        plan["forecast_error"] = str(error)
        return plan
    recommendation = decision["recommended_schedule"]
    if decision["fallback_reason"] or recommendation is None:
        plan["fallback_reason"] = decision["fallback_reason"] or "no_feasible_window"
        return plan
    plan.update(
        mode="carbon_aware",
        start=recommendation["start"].isoformat(),
        estimated_emissions_saved_g=decision["estimated_emissions_saved_g"],
        forecast_issued_at=scenario["forecast_issued_at"],
    )
    return plan


def build_daily_features(data_root, data_date):
    observations = {}
    for path in sorted((data_root / "raw" / "electricitymaps" / data_date).glob("*_FR.json")):
        row = json.loads(path.read_text(encoding="utf-8"))
        timestamp = utc_timestamp(row["datetime"])
        value = row["carbonIntensity"]
        if (
            row["zone"] != "FR"
            or timestamp.date().isoformat() != data_date
            or timestamp.minute or timestamp.second or timestamp.microsecond
            or not math.isfinite(value) or value < 0
        ):
            raise ValueError(f"Invalid hourly observation in {path.name}")
        if timestamp in observations and value != observations[timestamp]:
            raise ValueError(f"Conflicting observations for {timestamp.isoformat()}")
        observations[timestamp] = value
    if not observations:
        raise ValueError(f"No France electricity observations for {data_date}")
    values = list(observations.values())
    return {
        "grid_zone": "FR",
        "observation_date_utc": data_date,
        "observed_hours": len(values),
        "expected_hours": 24,
        "coverage_fraction": len(values) / 24,
        "mean_gco2e_kwh": statistics.mean(values),
        "min_gco2e_kwh": min(values),
        "max_gco2e_kwh": max(values),
    }


def run_workload(data_root, run_id, plan):
    data_date = date.fromisoformat(plan["data_date"]).isoformat()
    key = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    output_dir = data_root / "usage" / "daily_grid_features" / data_date
    output = output_dir / f"{key}.json"
    if output.is_file():
        return output
    started = datetime.now(timezone.utc)
    if started < utc_timestamp(plan["start"]) or started > utc_timestamp(plan["latest_start"]):
        raise RuntimeError("Workload start is outside its allowed execution window")
    timer = perf_counter()
    features = build_daily_features(data_root, data_date)
    output_dir.mkdir(parents=True, exist_ok=True)
    completed = datetime.now(timezone.utc)
    if completed >= utc_timestamp(plan["deadline"]):
        raise RuntimeError("Workload exceeded its completion deadline")
    result = {
        "run_id": run_id,
        "plan": plan,
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "runtime_seconds": perf_counter() - timer,
        "features": features,
    }
    with NamedTemporaryFile(dir=output_dir, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        temporary.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        if datetime.now(timezone.utc) >= utc_timestamp(plan["deadline"]):
            raise RuntimeError("Workload exceeded its deadline before publishing")
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_date", help="UTC observation date, YYYY-MM-DD")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    plan = plan_run({"data_date": args.data_date}, datetime.now(timezone.utc))
    print(run_workload(args.data_root, args.run_id, plan))


if __name__ == "__main__":
    main()
