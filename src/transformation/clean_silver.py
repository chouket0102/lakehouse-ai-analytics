from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def transform_bronze_to_silver(spark: SparkSession):
    # Load Bronze Data
    df_locations = spark.table("air_quality.bronze.air_quality_locations_bronze")
    df_sensors = spark.table("air_quality.bronze.air_quality_sensors_bronze")
    # 1. Explode sensors array in locations to flatten the relationship
    # We select relevant location fields and explode the 'sensors' array
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

    # 2. Parse Coverage Information from Sensors table
    # Define schema for the 'coverage' JSON column
    coverage_schema = StructType([
        StructField("expectedCount", DoubleType(), True),
        StructField("expectedInterval", StringType(), True),
        StructField("observedCount", DoubleType(), True),
        StructField("observedInterval", StringType(), True),
        StructField("percentComplete", DoubleType(), True),
        StructField("percentCoverage", DoubleType(), True),
        StructField("datetimeFrom", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True),
        StructField("datetimeTo", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True)
    ])

    # Select and Parse
    df_sensors_parsed = df_sensors.select(
        "id", "name", "parameter", "coverage", "datetimeFirst", "datetimeLast"
    ).withColumn(
        "coverage_struct", from_json("coverage", coverage_schema)
    )

    # Extract fields from the parsed struct and cast timestamps
    df_sensors_enriched = df_sensors_parsed.withColumn(
        "percent_coverage", col("coverage_struct.percentCoverage")
    ).withColumn(
        "observed_count", col("coverage_struct.observedCount")
    ).withColumn(
        "expected_count", col("coverage_struct.expectedCount")
    ).withColumn(
        "datetime_first", F.to_timestamp(col("datetimeFirst.utc"))
    ).withColumn(
        "datetime_last", F.to_timestamp(col("datetimeLast.utc"))
    )

    # 3. Join Location/Sensor Metadata with Sensor Coverage/Stats
    # Join on sensor_id
    df_metadata = df_sensors_flat.join(
        df_sensors_enriched,
        df_sensors_flat.sensor_id == df_sensors_enriched.id,
        how="left"
    ).drop(df_sensors_enriched.id) # Drop duplicate ID column

    # 4. Load and Enrich Measurements
    # We now join the actual measurements with our rich metadata
    df_measurements = spark.table("air_quality.bronze.air_quality_measurements_bronze")
    
    # Join measurements with metadata on sensor_id
    # We use a left join to keep all measurements, even if metadata is missing (though it shouldn't be)
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
        
        # Location Metadata
        col("location_id"),
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

    
    spark.sql("CREATE SCHEMA IF NOT EXISTS air_quality.silver")
    

    (df_silver.write
        .format("delta")
        .mode("overwrite") # Overwrite for now to refresh the schema
        .option("mergeSchema", "true")
        .saveAsTable("air_quality.silver.air_quality_measurements_silver")
    )
    
    print("✔ Successfully enriched and wrote to air_quality.silver.air_quality_measurements_silver")
