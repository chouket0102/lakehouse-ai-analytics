import time
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable
from src.data_ingestion.locations_ingest import safe_get, BASE_URL, spark, session  
from src.data_ingestion.models import measurements_schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.getOrCreate()

def _parse_measurement_result(sensor_id, r):
    # Replace all .get() with direct access
    # According to OpenAQ v3 docs, measurements use 'period' not 'date'
    parameter = r["parameter"] if "parameter" in r else {}
    parameter_name = parameter["name"] if isinstance(parameter, dict) and "name" in parameter else None
    parameter_units = parameter["units"] if isinstance(parameter, dict) and "units" in parameter else None

    # Extract from period.datetimeFrom (v3 API structure)
    period = r["period"] if "period" in r else {}
    datetime_from = period["datetimeFrom"] if "datetimeFrom" in period else {}
    datetime_utc = datetime_from["utc"] if "utc" in datetime_from else None
    datetime_local = datetime_from["local"] if "local" in datetime_from else None

    value = r["value"] if "value" in r else None
    try:
        if value is not None:
            value = float(value)
    except Exception:
        value = None

    location_id = None
    if "locationId" in r:
        location_id = r["locationId"]
    elif "location_id" in r:
        location_id = r["location_id"]

    return {
        "sensor_id": int(sensor_id),
        "location_id": location_id,
        "parameter": parameter_name,
        "value": value,
        "unit": parameter_units,
        "datetime_utc": datetime_utc,
        "datetime_local": datetime_local
    }

def fetch_air_quality_measurements(date_from=None, date_to=None):
    df_sensors = spark.table("bronze.air_quality_sensors_bronze")
    sensor_ids = [row.id for row in df_sensors.select("id").collect()]
    
    print(f"📊 Fetching measurements for {len(sensor_ids)} sensors...")
    
    # If no date range specified, get last 7 days to avoid overwhelming the API
    if not date_from and not date_to:
        from datetime import datetime, timedelta
        date_to = datetime.now().strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        print(f"📅 No date range specified. Using last 7 days: {date_from} to {date_to}")

    all_measurements = []
    successful_sensors = 0
    failed_sensors = 0
    
    for idx, sensor_id in enumerate(sensor_ids, 1):
        print(f"🔄 Processing sensor {idx}/{len(sensor_ids)}: {sensor_id}")
        page = 1
        limit = 1000
        sensor_measurement_count = 0
        
        while True:
            url = f"{BASE_URL}/sensors/{sensor_id}/measurements"
            params = {"limit": limit, "page": page}
            if date_from:
                params["date_from"] = date_from
            if date_to:
                params["date_to"] = date_to
            try:
                data = safe_get(url, params=params)
                # Replace .get() with direct access
                if "results" in data and data["results"]:
                    results = data["results"]
                else:
                    break
                for r in results:
                    parsed = _parse_measurement_result(sensor_id, r)
                    # only append if value and timestamp exist
                    if parsed["value"] is not None and parsed["datetime_utc"]:
                        all_measurements.append(parsed)
                        sensor_measurement_count += 1
                
                # Check if there are more pages
                if len(results) < limit:
                    break
                page += 1
                
            except Exception as e:
                print(f"   ⚠ Error on page {page} for sensor {sensor_id}: {e}")
                failed_sensors += 1
                break
        
        if sensor_measurement_count > 0:
            print(f"   ✅ Collected {sensor_measurement_count} measurements from sensor {sensor_id}")
            successful_sensors += 1
        else:
            print(f"   ⚠ No measurements found for sensor {sensor_id}")

    print(f"\n📈 Summary: {successful_sensors} sensors succeeded, {failed_sensors} sensors failed")
    print(f"📊 Total measurements collected: {len(all_measurements)}")

    if not all_measurements:
        print("⚠ No measurement data found.")
        return

    df_measurements = spark.createDataFrame(all_measurements, schema=measurements_schema)

    df_measurements = df_measurements \
        .withColumn("datetime_utc", to_timestamp(col("datetime_utc"))) \
        .withColumn("datetime_local", to_timestamp(col("datetime_local")))

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