from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, when, concat_ws
import os
import shutil
from datetime import datetime, timezone

def combine():
    spark = SparkSession.builder \
        .appName("combine_stress_score") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = "/data" if os.path.exists("/data") else "data"

    elec_dir = f"{base}/formatted/electricitymaps/{date_str}"
    water_dir = f"{base}/formatted/hubeau/{date_str}"
    output_dir = f"{base}/usage/stress_score/{date_str}"

    if not os.path.exists(elec_dir) or not os.path.exists(water_dir):
        print("Missing formatted data — run formatting jobs first")
        return

    # Read formatted parquet files
    df_elec = spark.read.parquet(elec_dir)
    df_water = spark.read.parquet(water_dir)

    # Get France carbon intensity (only FR zone)
    fr_carbon = df_elec.filter(col("zone") == "FR") \
        .select(
            col("carbon_intensity_gco2_kwh"),
            col("ai_query_cost_gco2"),
            col("timestamp_utc"),
            col("estimation_method")
        ).first()

    if not fr_carbon:
        print("No FR carbon data found")
        return

    # Add carbon intensity to each French water region
    df_combined = df_water.withColumn(
        "carbon_intensity_gco2_kwh",
        lit(float(fr_carbon["carbon_intensity_gco2_kwh"]))
    ).withColumn(
        "ai_query_cost_gco2",
        lit(float(fr_carbon["ai_query_cost_gco2"]))
    ).withColumn(
        "electricity_timestamp_utc",
        lit(str(fr_carbon["timestamp_utc"]))
    )

    # Compute energy stress score (0-1): France avg is ~60 gCO2/kWh
    df_combined = df_combined.withColumn(
        "energy_stress_score",
        round(col("carbon_intensity_gco2_kwh") / 60.0, 3)
    )

    # Compute water stress score (0-1): below 500=stressed, 500-2000=medium, above=normal
    df_combined = df_combined.withColumn(
        "water_stress_score",
        when(col("river_flow_l_s") < 500, lit(0.9))
        .when(col("river_flow_l_s") < 2000, lit(0.5))
        .otherwise(lit(0.1))
    )

    # Combined stress score = average of both
    df_combined = df_combined.withColumn(
        "combined_stress_score",
        round((col("energy_stress_score") + col("water_stress_score")) / 2.0, 3)
    )

    # Traffic light classification
    df_combined = df_combined.withColumn(
        "sustainability_signal",
        when(col("combined_stress_score") < 0.3, lit("🟢 GREEN — Safe to run AI"))
        .when(col("combined_stress_score") < 0.6, lit("🟡 YELLOW — Use with caution"))
        .otherwise(lit("🔴 RED — Defer AI tasks"))
    )

    # AI cost interpretation
    df_combined = df_combined.withColumn(
        "ai_cost_per_1000_queries_gco2",
        round(col("ai_query_cost_gco2") * 1000, 2)
    )

    # Select final columns
    df_final = df_combined.select(
        col("zone"),
        col("region_name"),
        col("carbon_intensity_gco2_kwh"),
        col("energy_stress_score"),
        col("river_flow_l_s"),
        col("water_level_status"),
        col("water_stress_score"),
        col("combined_stress_score"),
        col("sustainability_signal"),
        col("ai_query_cost_gco2"),
        col("ai_cost_per_1000_queries_gco2"),
        col("latitude"),
        col("longitude"),
        col("partition_date"),
        col("electricity_timestamp_utc")
    )

    # Save to usage layer
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    df_final.write.parquet(output_dir)

    print(f"Combined {df_final.count()} records → {output_dir}")
    df_final.show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    combine()