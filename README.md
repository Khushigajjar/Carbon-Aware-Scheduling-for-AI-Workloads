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
Carbon-Aware-Scheduling-for-AI-Workloads.code-workspace
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

## Live France forecast pilot

The shadow DAG can fetch France's hourly carbon-intensity forecast from the [Electricity Maps v5 API](https://app.electricitymaps.com/docs/reference/sustainability/carbon-intensity/forecasts/latest). Configure `ELECTRICITY_MAPS_TOKEN` in the local Compose environment with access to the France forecast, then recreate the scheduler so it receives the token.

```powershell
docker compose up -d --no-deps --force-recreate airflow-scheduler
```

For a pilot run, set `"forecast_source": "electricity_maps_v5"` at the top level of the trigger configuration and provide the `jobs` list. Supply each job's name, expected runtime in minutes, estimated energy in kWh, and timezone-aware submission time, fixed start, and completion deadline. Choose a fixed start in the future within the provider's 24-hour forecast horizon. The DAG fetches the forecast, applies the six-hour freshness check at decision time, and records the provider, zone, issue time, and decision time with the report. Filter Kibana to `data_origin: "live"`. Electricity savings are unavailable for this feed because it supplies carbon intensity only.

The shadow DAG continues to record recommendations without running jobs. The controlled workload below has also been verified with an authenticated France forecast. Energy is an input estimate, so the reported emissions difference is a projection.

## Run the controlled production workload

A November 27 replay used 25 hourly forecast values returned by Electricity Maps for France. For a one-minute workload with an illustrative energy estimate of 0.01 kWh, the scheduler moved the proposed start from 13:00 to 18:00 UTC and projected a 3.6% emissions reduction, with completion before the 19:00 UTC deadline. This test used a supplied forecast snapshot and did not execute the workload.

`daily_grid_features_production` runs an actual batch job. It reads stored French grid observations and publishes a daily feature record with mean, minimum, and maximum carbon intensity, observation coverage, and actual start, completion, and runtime. It fails on missing or contradictory observations and reports partial coverage explicitly.

To run the checked-in example from the repository root:

```powershell
python scripts/grid_feature_workload.py 2026-06-06 --run-id local-demo
```

In Airflow, unpause `daily_grid_features_production`, open its [trigger form](http://localhost:8080/dags/daily_grid_features_production/trigger), and submit:

```json
{"data_date": "2026-06-06"}
```

This manually triggered pilot uses the fixed schedule by default and starts as soon as Airflow is ready. Results go to `data/usage/daily_grid_features/<UTC date>/`, with one atomic output per run. The DAG waits for the selected start and refuses to start after the latest feasible time. Published results are reused on a retry of the same run.

Verified end to end in local Airflow on November 27, 2026. Run `production_verification_20261127` published a feature record for June 6, 2026, with 2 of 24 observation hours and a mean of 15 gCO₂e/kWh. Authenticated run `live_fr_pilot_1e465aea` fetched a live France forecast, selected a start in carbon-aware mode without falling back, and published before its deadline. Estimated emissions savings were zero because both starts fell in the same forecast hour.

To opt into carbon-aware execution, supply `"mode": "carbon_aware"`, a future timezone-aware `fixed_start`, a `deadline` within 24 hours, `runtime_minutes`, and a positive `energy_kwh` estimate along with the observation date. Configure the forecast token as described above. If the provider is unavailable, the data is stale, or there is insufficient forecast coverage, the workload uses the fixed start when the deadline guard still permits it. Run reports record the selected plan and actual timing. Energy is supplied as an estimate, and the workload does not measure consumption.

### Production dashboard

The `index_execution` task sends published run metrics to Elasticsearch's `grid-feature-executions` index. It retries independently from execution and uses one stable document ID per Airflow run. Import [grid-feature-production.ndjson](kibana/grid-feature-production.ndjson) through Kibana Saved Objects, then open [Grid Feature Production Runs](http://localhost:5601/app/dashboards#/view/grid-feature-production-dashboard). The four charts show fixed, selected, and actual starts, actual runtime and observation coverage, on-time publications, estimated emissions savings, and fallback counts. Use Airflow to inspect failed or unpublished runs.

The dashboard pipeline was verified locally on November 27, 2026. Airflow run `production_dashboard_9ab3ad34` completed all four tasks, fetched a live France forecast, and indexed an on-time publication. Kibana accepted the dashboard import. The short verification run produced zero estimated savings. The separate forecast replay illustrates the effect of shifting across hours.

The Docker Compose setup is a local pilot. A company deployment needs a managed runtime, secured service access, a commercial forecast entitlement where applicable, and an agreed alert destination and workload schedule. The DAG is manually triggered so an operator controls when it runs.

## Reliability checks

The [test runner](tests/shadow_reliability.py) covers missing, stale, and fresh forecasts, impossible deadlines, retry deduplication, and recovery after a local Elasticsearch outage. The [saved results](tests/shadow_reliability_results.json) record the verified outcomes. To run the checks against your local stack:

```powershell
python tests/shadow_reliability.py --output tests/shadow_reliability_results.json
```

Pass `--docker docker` to include the outage case, which briefly stops and restarts Elasticsearch in the local Compose stack.
