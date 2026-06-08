from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, round
import os
from datetime import datetime, timezone

def format_electricitymaps():
    spark = SparkSession.builder \
        .appName("format_electricitymaps") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = "/opt/airflow/data" if os.path.exists("/opt/airflow/data") else "/data" if os.path.exists("/data") else "data"

    input_dir = f"{base}/raw/electricitymaps/{date_str}"
    output_dir = f"{base}/formatted/electricitymaps/{date_str}"

    if not os.path.exists(input_dir):
        print(f"No raw data found for {date_str}")
        return

    df = spark.read.option("multiline", "true").json(f"{input_dir}/*.json")

    df_formatted = df.select(
        col("zone"),
        col("region_name"),
        col("carbonIntensity").cast("double").alias("carbon_intensity_gco2_kwh"),
        to_timestamp(col("datetime")).alias("timestamp_utc"),
        to_timestamp(col("fetched_at")).alias("fetched_at_utc"),
        col("emissionFactorType").alias("emission_factor_type"),
        col("estimationMethod").alias("estimation_method"),
        col("isEstimated").alias("is_estimated"),
        lit(date_str).alias("partition_date")
    )

    df_formatted = df_formatted.withColumn(
        "ai_query_cost_gco2",
        round(col("carbon_intensity_gco2_kwh") * 0.003, 4)
    )

    df_formatted.write.mode("overwrite").parquet(output_dir)
    print(f"Formatted {df_formatted.count()} records → {output_dir}")
    df_formatted.show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    format_electricitymaps()