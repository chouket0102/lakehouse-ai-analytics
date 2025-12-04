# Databricks notebook source
from src.transformation.clean_silver import transform_bronze_to_silver

# COMMAND ----------

if __name__ == "__main__":
    transform_bronze_to_silver(spark)
