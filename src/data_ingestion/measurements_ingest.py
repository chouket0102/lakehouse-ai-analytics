import time
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable
from src.data_ingestion.locations_ingest import safe_get, BASE_URL, spark, session  
from src.data_ingestion.models import measurements_schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.getOrCreate()

def _parse_measurement_result(sensor_id, r):

    parameter = r.get("parameter") or {}
    parameter_name = parameter.get("name") if isinstance(parameter, dict) else None
    parameter_units = parameter.get("units") if isinstance(parameter, dict) else None

    date = r.get("date") or {}
    datetime_utc = date.get("utc")
    datetime_local = date.get("local")

    value = r.get("value")
    try:
        if value is not None:
            value = float(value)
    except Exception:
        value = None

    return {
        "sensor_id": int(sensor_id),
        "location_id": r.get("locationId") or r.get("location_id") or None,
        "parameter": parameter_name,
        "value": value,
        "unit": parameter_units,
        "datetime_utc": datetime_utc,
        "datetime_local": datetime_local
    }

def fetch_air_quality_measurements(date_from=None, date_to=None):
    df_sensors = spark.table("bronze.air_quality_sensors_bronze")
    sensor_ids = [row.id for row in df_sensors.select("id").collect()]

    all_measurements = []
    for sensor_id in sensor_ids:
        page = 1
        limit = 1000
        while True:
            url = f"{BASE_URL}/sensors/{sensor_id}/measurements"
            params = {"limit": limit, "page": page}
            if date_from:
                params["date_from"] = date_from
            if date_to:
                params["date_to"] = date_to
            try:
                data = safe_get(url, params=params)
                results = data.get("results", [])
                if not results:
                    break
                for r in results:
                    parsed = _parse_measurement_result(sensor_id, r)
                    # only append if value and timestamp exist (you can relax this)
                    if parsed["value"] is not None and parsed["datetime_utc"]:
                        all_measurements.append(parsed)
                page += 1
            except Exception as e:
                print(f" Error fetching measurements for sensor {sensor_id}: {e}")
                break

    if not all_measurements:
        print(" No measurement data found.")
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
        print(f" Merged {len(all_measurements)} measurements into {table_name}")
    else:
        print(f"Creating new table {table_name}...")
        df_measurements.write.format("delta").saveAsTable(table_name)
        print(f" Created {table_name} with {len(all_measurements)} measurements")