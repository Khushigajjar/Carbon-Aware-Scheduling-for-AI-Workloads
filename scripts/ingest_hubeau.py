import requests
import json
import os
from datetime import datetime

REGIONS = {
    "FR-IDF": {"name": "Ile-de-France", "lat": 48.85, "lon": 2.35},
    "FR-ARA": {"name": "Auvergne-Rhone-Alpes", "lat": 45.75, "lon": 4.85},
    "FR-OCC": {"name": "Occitanie", "lat": 43.60, "lon": 1.44},
    "FR-NAQ": {"name": "Nouvelle-Aquitaine", "lat": 44.84, "lon": -0.58},
    "FR-HDF": {"name": "Hauts-de-France", "lat": 50.63, "lon": 3.06},
}
BASE_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"

def ingest():
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    output_dir = f"data/raw/hubeau/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/river_flows.json"

    if os.path.exists(output_file):
        print(f"Already exists, skipping: {output_file}")
        return

    results = {}

    for zone, info in REGIONS.items():
        response = requests.get(
            BASE_URL,
            params={
    "format": "json",
    "grandeur_hydro_elab": "QmM",
    "latitude": info["lat"],
    "longitude": info["lon"],
    "distance": 50,
    "size": 24,
    "date_debut_obs_elab": "2023-01-01T00:00:00Z",
    "date_fin_obs_elab": "2026-01-01T00:00:00Z"
}
        )

        if response.status_code in (200, 206):
            data = response.json()
            records = data.get("data", [])
            latest = records[-1] if records else None
            results[zone] = {
                    "region_name": info["name"],
                    "fetched_at": datetime.utcnow().isoformat(),
                    "data": [latest] if latest else []
}
            print(f"Saved flow data for {zone}")
        else:
            print(f"Error {response.status_code} for {zone}: {response.text}")

    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved: {output_file}")

if __name__ == "__main__":
    ingest()