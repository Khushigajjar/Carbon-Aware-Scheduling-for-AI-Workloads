# Carbon-Aware Scheduling for AI Workloads

This project recommends lower-carbon start times for flexible AI batch jobs while respecting their completion deadlines. It compares the recommendation with a fixed schedule and records projected emissions, electricity cost, and deadline compliance. Airflow runs the scheduler in shadow mode, so it records decisions without launching or rescheduling workloads.

## Demo results

The included synthetic scenario models two batch jobs. Both recommendations fit their deadlines. Compared with the fixed schedule, they reduce estimated combined emissions from 4.274 kg to 1.708 kg CO₂, a 60% reduction, and estimated electricity costs by €3.22.

The scheduler uses hourly carbon forecasts and assumes constant power throughout each job. Electricity prices are optional and help quantify the cost difference. Workloads are evaluated independently. Live forecast scenarios require an issuance timestamp when forecast data is present, and forecasts older than six hours at job submission are treated as stale.

## Run the simulator

From the repository root, run:

```powershell
python scripts/simulate_workloads.py examples/workload_scenario.json
```

## Run the Airflow and Kibana demo

1. With Docker running, initialize Airflow on a fresh checkout:

   ```powershell
   docker compose up -d postgres
   docker compose run --rm airflow-init
   ```

2. Start the services:

   ```powershell
   docker compose up -d airflow-webserver airflow-scheduler elasticsearch kibana
   ```

3. Sign in to [local Airflow](http://localhost:8080) with the demo account (`admin` / `admin`). Unpause `workload_scheduling_shadow` if needed, then open its [trigger form](http://localhost:8080/dags/workload_scheduling_shadow/trigger). Paste the full contents of [the sample scenario](examples/workload_scenario.json) into **Configuration JSON** and click **Trigger**.
4. Confirm that `record_recommendations` and `index_recommendations` succeed. The first task writes a report to `data/usage/workload_shadow/`. The second writes one Elasticsearch document per job per run to `workload-shadow-decisions` using stable IDs.
5. In [Kibana Saved Objects](http://localhost:5601/app/management/kibana/objects), import [the dashboard](kibana/workload-shadow.ndjson), then open **Workload Scheduling Shadow**. Set the time range to include the Airflow run. The sample has zero fallback decisions.

The separate Water–Energy Nexus pipeline uses Electricity Maps and Hub’Eau, with PySpark transformations and Elasticsearch indexing. Set `ELECTRICITY_MAPS_TOKEN` in the environment or an untracked `.env` file before running its electricity ingestion. Set `AIRFLOW__CORE__FERNET_KEY` to your environment’s key if you use encrypted Airflow settings or are reopening a database that already uses a key. The synthetic shadow demo takes its inputs from the scenario file.

## Reliability checks

The [test runner](tests/shadow_reliability.py) covers missing, stale, and fresh forecasts, impossible deadlines, retry deduplication, and recovery after a local Elasticsearch outage. The [saved results](tests/shadow_reliability_results.json) record the verified outcomes. To run the checks against your local stack:

```powershell
python tests/shadow_reliability.py --output tests/shadow_reliability_results.json
```

Pass `--docker docker` to include the outage case, which briefly stops and restarts Elasticsearch in the local Compose stack.
