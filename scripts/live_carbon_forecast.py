"""Build a French shadow scenario from an Electricity Maps v5 forecast."""

import json
import os
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from simulate_workloads import utc_timestamp


ENDPOINT = "https://api.electricitymaps.com/v5/carbon-intensity/forecasts/latest"


def build_live_scenario(scenario, payload, decision_at):
    if payload.get("zone") != "FR" or payload.get("temporalGranularity") != "hourly":
        raise ValueError("Expected an hourly Electricity Maps forecast for France")
    jobs = []
    for job in scenario["jobs"]:
        earliest_start = max(utc_timestamp(job["submitted_at"]), decision_at)
        if utc_timestamp(job["fixed_start"]) < earliest_start:
            raise ValueError(f'{job["name"]}: fixed start has passed or precedes submission')
        jobs.append(dict(job, submitted_at=earliest_start.isoformat()))

    forecast = []
    issue_times = []
    for point in payload["data"]:
        if point["value"] is None:
            continue
        hour = utc_timestamp(point["datetime"])
        issued_at = utc_timestamp(point["updatedAt"])
        if issued_at > decision_at:
            raise ValueError("Provider forecast was issued after the decision time")
        forecast.append({"hour": hour.isoformat(), "carbon_g_per_kwh": point["value"]})
        issue_times.append(issued_at)
    return {
        "data_origin": "live",
        "forecast_source": "electricity_maps_v5",
        "forecast_zone": "FR",
        "forecast_issued_at": min(issue_times).isoformat() if issue_times else None,
        "decision_at": decision_at.isoformat(),
        "forecast": forecast,
        "jobs": jobs,
    }


def fetch_live_scenario(scenario):
    token = os.environ.get("ELECTRICITY_MAPS_TOKEN")
    if not token:
        raise ValueError("Set ELECTRICITY_MAPS_TOKEN with France forecast access")
    query = urlencode({"zone": "FR", "temporalGranularity": "hourly", "horizonHours": 24})
    request = Request(f"{ENDPOINT}?{query}", headers={"auth-token": token})
    try:
        with urlopen(request, timeout=15) as response:
            payload = json.load(response)
    except HTTPError as error:
        raise ValueError(f"Electricity Maps forecast request failed with HTTP {error.code}") from None
    except URLError:
        raise ValueError("Electricity Maps forecast request could not connect") from None
    return build_live_scenario(scenario, payload, datetime.now(timezone.utc))
