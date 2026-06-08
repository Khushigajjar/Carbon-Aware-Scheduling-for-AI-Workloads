from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, when
import os
import json
from datetime import datetime, timezone

def format_hubeau():
    spark = SparkSession.builder \
        .appName("format_hubeau") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = "/opt/airflow/data" if os.path.exists("/opt/airflow/data") else "/data" if os.path.exists("/data") else "data"

    input_file = f"{base}/raw/hubeau/{date_str}/river_flows.json"
    output_dir = f"{base}/formatted/hubeau/{date_str}"

    if not os.path.exists(input_file):
        print(f"No raw data found for {date_str}")
        return

    with open(input_file, "r") as f:
        raw = json.load(f)

    rows = []
    for zone, content in raw.items():
        records = content.get("data", [])
        if records and records[0]:
            r = records[0]
            rows.append({
                "zone": zone,
                "region_name": content["region_name"],
                "fetched_at": content["fetched_at"],
                "station_code": r.get("code_station", ""),
                "date_obs": r.get("date_obs_elab", ""),
                "river_flow_l_s": float(r["resultat_obs_elab"]) if r.get("resultat_obs_elab") else None,
                "data_quality": r.get("libelle_qualification", ""),
                "longitude": float(r.get("longitude", 0)),
                "latitude": float(r.get("latitude", 0)),
            })

    df = spark.createDataFrame(rows)

    df_formatted = df.select(
        col("zone"),
        col("region_name"),
        col("station_code"),
        col("river_flow_l_s"),
        to_timestamp(col("date_obs")).alias("observation_date_utc"),
        to_timestamp(col("fetched_at")).alias("fetched_at_utc"),
        col("data_quality"),
        col("latitude"),
        col("longitude"),
        lit(date_str).alias("partition_date"),
        when(col("river_flow_l_s") < 500, "low")
        .when(col("river_flow_l_s") < 2000, "medium")
        .otherwise("normal").alias("water_level_status")
    )

    df_formatted.write.mode("overwrite").parquet(output_dir)
    print(f"Formatted {df_formatted.count()} records → {output_dir}")
    df_formatted.show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    format_hubeau()