import time
from pyspark.sql import SparkSession, Row
from delta.tables import DeltaTable
from pyspark.sql.functions import explode, col
from src.data_ingestion.locations_ingest import safe_get, BASE_URL, spark, session  
from src.data_ingestion.locations_ingest import location_schema  
from src.data_ingestion.models import sensor_schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType

spark = SparkSession.builder.getOrCreate()

def fetch_air_quality_sensors():
    table_name = "bronze.air_quality_locations_bronze"
    df_locations = spark.table(table_name)

    sensor_rows = df_locations.select(explode("sensors").alias("sensor")).select("sensor.*").distinct().collect()
    # Fix: Row objects don't support .get(), use direct attribute access
    sensor_ids = [r["id"] for r in sensor_rows if r and "id" in r.asDict()]

    all_sensor_rows = []
    for sensor_id in sensor_ids:
        url = f"{BASE_URL}/sensors/{sensor_id}"
        try:
            json_data = safe_get(url)
            # Replace .get() with direct access
            if "results" in json_data:
                results = json_data["results"]
            else:
                results = []
            if isinstance(results, dict):
                results = [results]
            for item in results:
                all_sensor_rows.append(item)
        except Exception as e:
            print(f" Error fetching sensor {sensor_id}: {e}")

    if not all_sensor_rows:
        print(" No sensor data found.")
        return

    df_sensors = spark.createDataFrame([Row(**r) for r in all_sensor_rows], schema=sensor_schema)

    table_name = "bronze.air_quality_sensors_bronze"
    if spark.catalog.tableExists(table_name):
        print(f"Merging updates into {table_name}...")
        delta_table = DeltaTable.forName(spark, table_name)
        (delta_table.alias("old")
            .merge(df_sensors.alias("new"), "old.id = new.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f" Merged {len(all_sensor_rows)} sensors into {table_name}")
    else:
        print(f"Creating new table {table_name}...")
        df_sensors.write.format("delta").saveAsTable(table_name)
        print(f" Created {table_name} with {len(all_sensor_rows)} sensors")


