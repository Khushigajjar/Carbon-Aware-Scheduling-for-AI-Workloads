import requests
import json
import os
from datetime import datetime

TOKEN = os.environ.get("ELECTRICITY_MAPS_TOKEN", "C9WBF5z3XDr79Kv8TCQa")

REGIONS = {
    "FR": {"name": "France", "lat": 48.85, "lon": 2.35},
}
BASE_URL = "https://api.electricitymap.org/v3/carbon-intensity/latest"

def ingest():
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    hour_str = datetime.utcnow().strftime("%H00")

    for zone, info in REGIONS.items():
        output_dir = f"data/raw/electricitymaps/{date_str}"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/{hour_str}_{zone}.json"

        if os.path.exists(output_file):
            print(f"Already exists, skipping: {output_file}")
            continue

        response = requests.get(
            BASE_URL,
            params={"zone": zone},
            headers={"auth-token": TOKEN}
        )

        if response.status_code == 200:
            data = response.json()
            data["zone"] = zone
            data["region_name"] = info["name"]
            data["fetched_at"] = datetime.utcnow().isoformat()

            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Saved: {output_file}")
        else:
            print(f"Error {response.status_code} for zone {zone}: {response.text}")

if __name__ == "__main__":
    ingest()