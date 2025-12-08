from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode

def transform_bronze_to_silver(spark: SparkSession):
    # Load Bronze Data
    spark.sql("USE CATALOG air_quality")
    df_locations = spark.table("bronze.air_quality_locations_bronze") 
    df_sensors = spark.table("bronze.air_quality_sensors_bronze")
    
    # 1. Explode sensors array in locations to flatten the relationship
    df_sensors_flat = df_locations.select(
        "id", "name", "locality", "timezone", "country", "coordinates",
        explode("sensors").alias("sensor")
    ).select(
        col("id").alias("location_id"),
        col("name").alias("location_name"),
        col("locality"),
        col("timezone"),
        col("country.code").alias("country_code"),
        col("coordinates.latitude").alias("latitude"),
        col("coordinates.longitude").alias("longitude"),
        col("sensor.id").alias("sensor_id"),
        col("sensor.name").alias("sensor_name"),
        col("sensor.parameter.id").alias("parameter_id"),
        col("sensor.parameter.name").alias("parameter_name"),
        col("sensor.parameter.units").alias("parameter_units"),
        col("sensor.parameter.displayName").alias("parameter_display_name")
    )

    # 2. Extract fields directly from the struct
    # FIX: Removed "parameter" from this list to avoid duplication/ambiguity later
    df_sensors_enriched = df_sensors.select(
        "id", "name", "coverage", "datetimeFirst", "datetimeLast" 
    ).withColumn(
        "percent_coverage", col("coverage.percentCoverage")
    ).withColumn(
        "observed_count", col("coverage.observedCount")
    ).withColumn(
        "expected_count", col("coverage.expectedCount")
    ).withColumn(
        "datetime_first", F.to_timestamp(col("datetimeFirst.utc"))
    ).withColumn(
        "datetime_last", F.to_timestamp(col("datetimeLast.utc"))
    )

    # 3. Join Location/Sensor Metadata with Sensor Coverage/Stats
    df_metadata = df_sensors_flat.join(
        df_sensors_enriched,
        df_sensors_flat.sensor_id == df_sensors_enriched.id,
        how="left"
    ).drop(df_sensors_enriched.id) 

    # 4. Load and Enrich Measurements
    df_measurements = spark.table("bronze.air_quality_measurements_bronze").alias("m")
    df_metadata = df_metadata.alias("meta")
    
    # Join measurements with metadata on sensor_id
    # We join on the sensor_id, which is unambiguous
    df_silver = df_measurements.join(
        df_metadata,
        on="sensor_id",
        how="left"
    )

    # Select and reorder columns for the final Silver table
    df_silver = df_silver.select(
        # Measurement Data
        col("datetime_utc"),
        col("datetime_local"),
        col("value"),
        col("unit"),
        col("parameter"),
        
        col("meta.location_id").alias("location_id"), 
        col("location_name"),
        col("locality").alias("city"),
        col("country_code"),
        col("latitude"),
        col("longitude"),
        col("timezone"),
        
        # Sensor Metadata
        col("sensor_id"),
        col("sensor_name"),
        col("parameter_display_name"),
        col("percent_coverage")
    )

    (df_silver.write
        .format("delta")
        .mode("overwrite") 
        .option("mergeSchema", "true")
        .saveAsTable("silver.air_quality_measurements_silver")
    )
    
    print("Successfully enriched and wrote to silver.air_quality_measurements_silver")

