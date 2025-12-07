# Databricks notebook source
spark.sql("CREATE CATALOG IF NOT EXISTS air_quality")
spark.sql("USE CATALOG air_quality")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

from src.data_ingestion.locations_ingest import fetch_air_quality_locations
from src.data_ingestion.sensors_ingest import fetch_air_quality_sensors
from src.data_ingestion.measurements_ingest import fetch_air_quality_measurements
from src.transformation.clean_silver import transform_bronze_to_silver
from src.transformation.gold_agg import transform_silver_to_gold

# COMMAND ----------

# data ingestion
fetch_air_quality_locations()
fetch_air_quality_sensors()
fetch_air_quality_measurements()

# COMMAND ----------

# silver transformation
transform_bronze_to_silver(spark)

# COMMAND ----------

# gold aggregation
transform_silver_to_gold(spark)
