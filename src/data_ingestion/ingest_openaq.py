import os
import requests
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, to_timestamp
from src.data_ingestion.models import *

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Load environment variables / API keys
location_id = 5886886
OPENAQ_API_KEY = "bb3f7808b0e6125c0b8e0c8c15980baeb7c19c92f0a956d06e3caa08f7234476"


# --------------------------
# FETCH LOCATIONS AND SAVE IN BRONZE
# --------------------------
def fetch_air_quality_locations():
    url = f"https://api.openaq.org/v3/locations/{location_id}"
    headers = {"X-API-Key": OPENAQ_API_KEY}

    response = requests.get(url, headers=headers)
    json_data = response.json()
    results = json_data["results"]

    # Normalize inconsistent fields
    for r in results:
        if "licenses" not in r or r["licenses"] is None:
            r["licenses"] = []
        if isinstance(r["licenses"], dict):
            r["licenses"] = [r["licenses"]]
        if "sensors" not in r or r["sensors"] is None:
            r["sensors"] = []
        if "instruments" not in r or r["instruments"] is None:
            r["instruments"] = []
        if "bounds" not in r:
            r["bounds"] = []

    # Convert dicts into Rows
    rows = [Row(**r) for r in results]

    # Create DataFrame with strict schema
    df = spark.createDataFrame(rows, schema=location_schema)

    # Write to bronze table
    df.write.mode("overwrite").format("delta").saveAsTable("bronze.air_quality_locations_bronze")
    print("✔ Saved cleaned location data into bronze.air_quality_locations_bronze")


# --------------------------
# FETCH SENSOR DATA AND SAVE IN BRONZE
# --------------------------
def fetch_air_quality_sensors():
    df_locations = spark.table("bronze.air_quality_locations_bronze")

    sensor_ids = (
        df_locations
        .select(explode("sensors").alias("sensor"))
        .select(col("sensor.id").alias("sensor_id"))
        .distinct()
        .collect()
    )
    sensor_ids = [row.sensor_id for row in sensor_ids]

    all_sensor_rows = []

    for sensor in sensor_ids:
        url = f"https://api.openaq.org/v3/sensors/{sensor}"
        headers = {"X-API-Key": OPENAQ_API_KEY}

        response = requests.get(url, headers=headers)
        response_json = response.json()

        for item in response_json.get("results", []):
            all_sensor_rows.append(item)

    df_sensors = spark.createDataFrame(all_sensor_rows, schema=sensor_schema)
    df_sensors.write.mode("overwrite").format("delta").saveAsTable("bronze.air_quality_sensors_bronze")
    print("✔ Saved sensors data into bronze.air_quality_sensors_bronze")


# --------------------------
# FETCH MEASUREMENTS AND SAVE IN SILVER
# --------------------------
def fetch_air_quality_measurements(date_from=None, date_to=None):
    df_sensors = spark.table("bronze.air_quality_sensors_bronze")

    sensor_ids = [row.id for row in df_sensors.select("id").collect()]
    all_measurements = []

    for sensor_id in sensor_ids:
        page = 1
        limit = 1000
        while True:
            url = f"https://api.openaq.org/v3/measurements?sensor_id={sensor_id}&limit={limit}&page={page}"
            if date_from:
                url += f"&date_from={date_from}"
            if date_to:
                url += f"&date_to={date_to}"

            headers = {"X-API-Key": OPENAQ_API_KEY}
            response = requests.get(url, headers=headers)
            data = response.json()
            results = data.get("results", [])
            if not results:
                break

            for r in results:
                all_measurements.append({
                    "sensor_id": sensor_id,
                    "location_id": r.get("locationId") or r.get("location_id"),
                    "parameter": r["parameter"]["name"],
                    "value": r["value"],
                    "unit": r["unit"],
                    "datetime_utc": r["date"]["utc"],
                    "datetime_local": r["date"]["local"]
                })
            page += 1

    # Create Spark DataFrame
    df_measurements = spark.createDataFrame(all_measurements, schema=measurements_schema)

    # Convert datetime strings to timestamps
    df_measurements = df_measurements \
        .withColumn("datetime_utc", to_timestamp(col("datetime_utc"))) \
        .withColumn("datetime_local", to_timestamp(col("datetime_local")))

    # Save as silver table
    df_measurements.write.mode("overwrite").format("delta").saveAsTable("bronze.air_quality_measurements_bronze")
    print("✔ Saved air quality measurements into silver.air_quality_measurements_tn")
