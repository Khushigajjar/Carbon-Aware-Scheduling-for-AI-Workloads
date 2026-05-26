import json
import os
from datetime import datetime, timezone
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
import shutil

def index_to_elasticsearch():
    # Connect to Elasticsearch
    es = Elasticsearch("http://elasticsearch:9200")

    if not es.ping():
        print("Cannot connect to Elasticsearch")
        return

    print("Connected to Elasticsearch")

    # Create index with mapping
    index_name = "stress_scores"

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")

    mapping = {
        "mappings": {
            "properties": {
                "zone": {"type": "keyword"},
                "region_name": {"type": "keyword"},
                "carbon_intensity_gco2_kwh": {"type": "float"},
                "energy_stress_score": {"type": "float"},
                "river_flow_l_s": {"type": "float"},
                "water_level_status": {"type": "keyword"},
                "water_stress_score": {"type": "float"},
                "combined_stress_score": {"type": "float"},
                "sustainability_signal": {"type": "keyword"},
                "ai_query_cost_gco2": {"type": "float"},
                "ai_cost_per_1000_queries_gco2": {"type": "float"},
                "location": {"type": "geo_point"},
                "partition_date": {"type": "date"},
                "electricity_timestamp_utc": {"type": "date"},
                "indexed_at": {"type": "date"}
            }
        }
    }

    es.indices.create(index=index_name, body=mapping)
    print(f"Created index: {index_name}")

    # Read parquet using Spark
    spark = SparkSession.builder \
        .appName("index_elasticsearch") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = "/data" if os.path.exists("/data") else "data"
    input_dir = f"{base}/usage/stress_score/{date_str}"

    if not os.path.exists(input_dir):
        print(f"No usage data found for {date_str}")
        return

    df = spark.read.parquet(input_dir)
    records = df.collect()

    # Index each record
    indexed = 0
    for row in records:
        doc = {
            "zone": row["zone"],
            "region_name": row["region_name"],
            "carbon_intensity_gco2_kwh": row["carbon_intensity_gco2_kwh"],
            "energy_stress_score": row["energy_stress_score"],
            "river_flow_l_s": row["river_flow_l_s"],
            "water_level_status": row["water_level_status"],
            "water_stress_score": row["water_stress_score"],
            "combined_stress_score": row["combined_stress_score"],
            "sustainability_signal": row["sustainability_signal"],
            "ai_query_cost_gco2": row["ai_query_cost_gco2"],
            "ai_cost_per_1000_queries_gco2": row["ai_cost_per_1000_queries_gco2"],
            "location": {
                "lat": row["latitude"],
                "lon": row["longitude"]
            },
            "partition_date": row["partition_date"],
            "electricity_timestamp_utc": str(row["electricity_timestamp_utc"]).replace(" ", "T"),
            "indexed_at": datetime.now(timezone.utc).isoformat()
        }

        es.index(index=index_name, body=doc)
        indexed += 1
        print(f"Indexed: {row['region_name']} — {row['sustainability_signal']}")

    print(f"\nTotal indexed: {indexed} records into '{index_name}'")
    spark.stop()

if __name__ == "__main__":
    index_to_elasticsearch()