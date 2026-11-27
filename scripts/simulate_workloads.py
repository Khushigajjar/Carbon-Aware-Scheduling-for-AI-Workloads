"""Compare a fixed batch schedule with carbon-aware recommendations."""

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional


HOUR = timedelta(hours=1)
MAX_FORECAST_AGE = timedelta(hours=6)


@dataclass(frozen=True)
class Job:
    name: str
    runtime_minutes: int
    energy_kwh: float
    submitted_at: datetime
    deadline: datetime
    fixed_start: datetime


@dataclass(frozen=True)
class Forecast:
    carbon_g_per_kwh: float
    price_eur_per_kwh: Optional[float] = None


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime
    meets_deadline: bool
    emissions_g: Optional[float]
    electricity_cost_eur: Optional[float]


def utc_timestamp(value):
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.utcoffset() is None:
        raise ValueError("Timestamps must include a UTC offset")
    return timestamp.astimezone(timezone.utc)


def hour_floor(timestamp):
    return timestamp.replace(minute=0, second=0, microsecond=0)


def estimate(job, start, forecast):
    runtime = timedelta(minutes=job.runtime_minutes)
    end = start + runtime
    cursor = start
    carbon = 0.0
    cost = 0.0
    has_prices = True
    while cursor < end:
        hour = hour_floor(cursor)
        sample = forecast.get(hour)
        if sample is None:
            return Window(start, end, end <= job.deadline, None, None)
        interval_end = min(end, hour + HOUR)
        energy = job.energy_kwh * ((interval_end - cursor) / runtime)
        carbon += energy * sample.carbon_g_per_kwh
        if sample.price_eur_per_kwh is None:
            has_prices = False
        else:
            cost += energy * sample.price_eur_per_kwh
        cursor = interval_end
    return Window(start, end, end <= job.deadline, carbon, cost if has_prices else None)


def difference(baseline, proposed):
    if baseline is None or proposed is None:
        return None
    return round(baseline - proposed, 6)


def recommend(job: Job, forecast: Dict[datetime, Forecast], forecast_issue=None):
    if forecast_issue:
        forecast = {}
    baseline = estimate(job, job.fixed_start, forecast)
    runtime = timedelta(minutes=job.runtime_minutes)
    latest_start = job.deadline - runtime
    candidates = {job.submitted_at, latest_start, job.fixed_start}
    # With uniform power, emissions change linearly between times at which
    # either end of the run crosses an hourly forecast boundary.
    for hour in forecast:
        for boundary in (hour, hour + HOUR):
            candidates.update((boundary, boundary - runtime))

    windows = (
        estimate(job, start, forecast)
        for start in candidates
        if job.submitted_at <= start <= latest_start
    )
    best = min(
        (window for window in windows if window.emissions_g is not None),
        key=lambda window: (window.emissions_g, window.start),
        default=None,
    )
    fallback_reason = None
    if best is None:
        fallback_reason = (
            "runtime_exceeds_available_time"
            if latest_start < job.submitted_at
            else forecast_issue or "insufficient_forecast_coverage"
        )
        best = baseline if baseline.meets_deadline else None

    return {
        "job_name": job.name,
        "fixed_schedule": asdict(baseline),
        "recommended_schedule": asdict(best) if best else None,
        "fallback_reason": fallback_reason,
        "estimated_emissions_saved_g": (
            difference(baseline.emissions_g, best.emissions_g) if best else None
        ),
        "estimated_electricity_cost_saved_eur": (
            difference(baseline.electricity_cost_eur, best.electricity_cost_eur)
            if best else None
        ),
    }


def simulate(scenario):
    issued_at = scenario.get("forecast_issued_at")
    issued_at = utc_timestamp(issued_at) if issued_at is not None else None
    decision_at = scenario.get("decision_at")
    decision_at = utc_timestamp(decision_at) if decision_at is not None else None
    forecast = {}
    for row in scenario["forecast"]:
        hour = utc_timestamp(row["hour"])
        carbon = row["carbon_g_per_kwh"]
        price = row.get("price_eur_per_kwh")
        if hour != hour_floor(hour) or hour in forecast:
            raise ValueError("Forecast timestamps must identify distinct UTC hours")
        if not math.isfinite(carbon) or carbon < 0:
            raise ValueError("Carbon intensity must be finite and nonnegative")
        if price is not None and not math.isfinite(price):
            raise ValueError("Electricity price must be finite")
        forecast[hour] = Forecast(carbon, price)

    decisions = []
    for row in scenario["jobs"]:
        job = Job(
            name=row["name"],
            runtime_minutes=row["runtime_minutes"],
            energy_kwh=row["energy_kwh"],
            submitted_at=utc_timestamp(row["submitted_at"]),
            deadline=utc_timestamp(row["deadline"]),
            fixed_start=utc_timestamp(row["fixed_start"]),
        )
        if job.runtime_minutes <= 0 or not math.isfinite(job.energy_kwh) or job.energy_kwh <= 0:
            raise ValueError(f"{job.name}: runtime and energy must be positive")
        if job.fixed_start < job.submitted_at or job.deadline <= job.submitted_at:
            raise ValueError(f"{job.name}: invalid submission, fixed start or deadline")
        forecast_issue = None
        # Replay the information available when the job was submitted.
        if issued_at is not None:
            age = (decision_at or job.submitted_at) - issued_at
            if age < timedelta(0):
                forecast_issue = "forecast_issued_after_submission"
            elif age > MAX_FORECAST_AGE:
                forecast_issue = "stale_forecast"
        elif forecast and scenario.get("data_origin") == "live":
            forecast_issue = "missing_forecast_issuance"
        decisions.append(recommend(job, forecast, forecast_issue))
    return decisions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", type=Path, help="JSON file containing jobs and hourly forecasts")
    args = parser.parse_args()
    try:
        scenario = json.loads(args.scenario.read_text(encoding="utf-8"))
        decisions = simulate(scenario)
    except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
        parser.error(str(error))
    print(json.dumps(decisions, indent=2, default=datetime.isoformat, allow_nan=False))


if __name__ == "__main__":
    main()
