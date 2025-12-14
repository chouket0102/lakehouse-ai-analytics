from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def transform_silver_to_gold(spark: SparkSession):
    df_silver = spark.table("air_quality.silver.air_quality_measurements_silver")

    df_daily = df_silver.groupBy(
        "location_id",
        "location_name",
        "city",
        "country_code",
        "parameter",
        "unit",
        F.to_date("datetime_local").alias("measurement_date")
    ).agg(
        F.round(F.avg("value"), 2).alias("avg_value"),
        F.max("value").alias("max_value"),
        F.min("value").alias("min_value"),
        F.count("value").alias("record_count"),
        F.first("latitude").alias("latitude"),
        F.first("longitude").alias("longitude")
    )

    df_gold = df_daily.withColumn(
        "health_risk",
        F.when(F.col("max_value") < 60, "Low (Good)")
         .when((F.col("max_value") >= 60) & (F.col("max_value") < 100), "Moderate")
         .when((F.col("max_value") >= 100) & (F.col("max_value") < 180), "High (Unhealthy)")
         .otherwise("Hazardous")
    )
    spark.sql("CREATE SCHEMA IF NOT EXISTS air_quality.gold")
    
    (df_gold.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("air_quality.gold.daily_air_quality_summary")
    )
    
    print("Gold Table Created: air_quality.gold.daily_air_quality_summary")
