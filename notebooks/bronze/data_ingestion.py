# Databricks notebook source
from src.data_ingestion.ingest_openaq import fetch_air_quality_locations, fetch_air_quality_sensors, fetch_air_quality_measurements

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS air_quality")
spark.sql("USE CATALOG air_quality")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# COMMAND ----------

fetch_air_quality_locations()
fetch_air_quality_sensors()
fetch_air_quality_measurements()
