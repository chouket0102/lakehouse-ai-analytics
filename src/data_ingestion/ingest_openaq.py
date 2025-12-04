import os
import requests
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, to_timestamp
from src.data_ingestion.models import *
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

OPENAQ_API_KEY = ""
location_ids=[2162939,2162982,2162940,2162946,2162945,2162942,2162943,2162944,2162947,2162981,2162980,2162989,2162965]

# --------------------------
# FETCH LOCATIONS AND SAVE IN BRONZE
# --------------------------
def fetch_air_quality_locations():
    all_rows = []
    
    for loc_id in location_ids:
        url = f"https://api.openaq.org/v3/locations/{loc_id}"
        headers = {"X-API-Key": OPENAQ_API_KEY}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            json_data = response.json()
            results = json_data.get("results", [])
            
            # If results is a dict (single object), wrap it in a list
            if isinstance(results, dict):
                results = [results]

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
            all_rows.extend([Row(**r) for r in results])
            
        except Exception as e:
            print(f"⚠ Error fetching location {loc_id}: {e}")

    if not all_rows:
        print("⚠ No location data found.")
        return

    # Create DataFrame with strict schema
    df_new = spark.createDataFrame(all_rows, schema=location_schema)

    # Idempotent Write (Merge/Upsert)
    table_name = "bronze.air_quality_locations_bronze"
    
    if spark.catalog.tableExists(table_name):
        print(f"Merging updates into {table_name}...")
        delta_table = DeltaTable.forName(spark, table_name)
        (delta_table.alias("old")
            .merge(
                df_new.alias("new"),
                "old.id = new.id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"✔ Merged {len(all_rows)} locations into {table_name}")
    else:
        print(f"Creating new table {table_name}...")
        df_new.write.format("delta").saveAsTable(table_name)
        print(f"✔ Created {table_name} with {len(all_rows)} locations")


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

    # Create DataFrame with strict schema
    df_sensors = spark.createDataFrame(all_sensor_rows, schema=sensor_schema)
    
    # Idempotent Write (Merge/Upsert)
    table_name = "bronze.air_quality_sensors_bronze"
    
    if spark.catalog.tableExists(table_name):
        print(f"Merging updates into {table_name}...")
        delta_table = DeltaTable.forName(spark, table_name)
        (delta_table.alias("old")
            .merge(
                df_sensors.alias("new"),
                "old.id = new.id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"✔ Merged {len(all_sensor_rows)} sensors into {table_name}")
    else:
        print(f"Creating new table {table_name}...")
        df_sensors.write.format("delta").saveAsTable(table_name)
        print(f"✔ Created {table_name} with {len(all_sensor_rows)} sensors")


# --------------------------
# FETCH MEASUREMENTS AND SAVE IN BRONZE
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
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
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
            except Exception as e:
                print(f"⚠ Error fetching measurements for sensor {sensor_id}: {e}")
                break

    if not all_measurements:
        print("⚠ No measurement data found.")
        return

    # Create Spark DataFrame
    df_measurements = spark.createDataFrame(all_measurements, schema=measurements_schema)

    # Convert datetime strings to timestamps
    df_measurements = df_measurements \
        .withColumn("datetime_utc", to_timestamp(col("datetime_utc"))) \
        .withColumn("datetime_local", to_timestamp(col("datetime_local")))

    # Idempotent Write (Merge/Upsert)
    # Deduplicate based on sensor_id AND timestamp
    table_name = "bronze.air_quality_measurements_bronze"
    
    if spark.catalog.tableExists(table_name):
        print(f"Merging updates into {table_name}...")
        delta_table = DeltaTable.forName(spark, table_name)
        (delta_table.alias("old")
            .merge(
                df_measurements.alias("new"),
                "old.sensor_id = new.sensor_id AND old.datetime_utc = new.datetime_utc"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"✔ Merged {len(all_measurements)} measurements into {table_name}")
    else:
        print(f"Creating new table {table_name}...")
        df_measurements.write.format("delta").saveAsTable(table_name)
        print(f"✔ Created {table_name} with {len(all_measurements)} measurements")
