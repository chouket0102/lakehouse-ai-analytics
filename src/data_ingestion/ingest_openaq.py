import os
import requests
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from src.data_ingestion.models import *
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

OPENAQ_API_KEY = ""
location_ids=[2162939,2162982,2162940,2162946,2162945,2162942,2162943,2162944,2162947,216298

1,2162980,2162989,2162965]

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
            
            # Handle results - could be a list or dict
            if "results" in json_data:
                results = json_data["results"]
            else:
                results = []
            
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
    
    print(f"Found {len(sensor_ids)} unique sensor IDs to fetch.")

    all_sensor_rows = []

    for sensor in sensor_ids:
        url = f"https://api.openaq.org/v3/sensors/{sensor}"
        headers = {"X-API-Key": OPENAQ_API_KEY}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            response_json = response.json()
            
            # Check if results exist and is not None
            if response_json and "results" in response_json:
                results = response_json["results"]
                if results:
                    for item in results:
                        all_sensor_rows.append(item)
        except Exception as e:
            print(f"⚠ Error fetching sensor {sensor}: {e}")
            continue

    if not all_sensor_rows:
        print("⚠ No sensor data found.")
       return

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
# Update schema to match OpenAQ v3 API response structure
measurements_schema_ingest = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("parameter", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("datetime_utc", StringType(), True),   # Extracted from period.datetimeFrom.utc
    StructField("datetime_local", StringType(), True)  # Extracted from period.datetimeFrom.local
])

def fetch_air_quality_measurements(date_from=None, date_to=None):
    # 1. Debug: Ensure we actually have sensors to query
    try:
        df_sensors = spark.table("bronze.air_quality_sensors_bronze")
        sensor_ids = [row.id for row in df_sensors.select("id").distinct().collect()]
        print(f"Found {len(sensor_ids)} sensors to query.")
    except Exception as e:
        print("CRITICAL: Could not read sensor table. Run fetch_air_quality_sensors() first.")
        return

    all_measurements = []

    for sensor_id in sensor_ids:
        page = 1
        limit = 1000
        while True:
            # Use the direct sensor endpoint as per documentation
            url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements?limit={limit}&page={page}"
            
            if date_from:
                url += f"&date_from={date_from}"
            if date_to:
                url += f"&date_to={date_to}"

            headers = {"X-API-Key": OPENAQ_API_KEY}
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                # Check if results exist
                if "results" in data and data["results"]:
                    results = data["results"]
                else:
                    break # Stop if no more data

                for r in results:
                    try:
                        # Extract dates from the 'period' object as per v3 docs
                        period = r["period"] if "period" in r else None
                        dt_from = period["datetimeFrom"] if period and "datetimeFrom" in period else None
                        
                        # Extract parameter info
                        param_info = r["parameter"] if "parameter" in r else None
                        
                        # Build measurement dict with safe access
                        measurement = {
                            "sensor_id": sensor_id,
                            "location_id": r["locationId"] if "locationId" in r else None,
                            "parameter": param_info["name"] if param_info and "name" in param_info else None,
                            "value": r["value"] if "value" in r else None,
                            "unit": param_info["units"] if param_info and "units" in param_info else None,
                            "datetime_utc": dt_from["utc"] if dt_from and "utc" in dt_from else None,
                            "datetime_local": dt_from["local"] if dt_from and "local" in dt_from else None
                        }
                        
                        all_measurements.append(measurement)
                    except (KeyError, TypeError) as e:
                        print(f"⚠ Skipping malformed measurement: {e}")
                        continue
                
                # Simple pagination safety break
                if len(results) < limit:
                    break
                page += 1
                
            except Exception as e:
                print(f"⚠ Error fetching measurements for sensor {sensor_id}: {e}")
                break

    if not all_measurements:
        print("⚠ No measurement data found (check Date range).")
        return

    # 4. Create DataFrame using String schema first
    df_measurements = spark.createDataFrame(all_measurements, schema=measurements_schema_ingest)

    # 5. Convert Strings to Timestamp
    df_measurements = df_measurements \
        .withColumn("datetime_utc", to_timestamp(col("datetime_utc"))) \
        .withColumn("datetime_local", to_timestamp(col("datetime_local")))

    # Idempotent Write
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
        print(f"✔ Merged {len(all_measurements)} measurements.")
    else:
        print(f"Creating new table {table_name}...")
        df_measurements.write.format("delta").saveAsTable(table_name)
        print(f"✔ Created {table_name} with {len(all_measurements)} measurements.")
