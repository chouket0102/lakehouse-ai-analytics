from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    ArrayType, BooleanType,  DoubleType, TimestampType
)

#Schema for OpenAQ Locations
licenses_schema = ArrayType(
    StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("attribution", StructType([
            StructField("name", StringType(), True),
            StructField("url", StringType(), True)
        ]), True),
        StructField("dateFrom", StringType(), True),
        StructField("dateTo", StringType(), True)
    ])
)

location_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("timezone", StringType(), True),

    StructField("country", StructType([
        StructField("id", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", StringType(), True),
    ]), True),

    StructField("owner", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]), True),

    StructField("provider", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]), True),

    StructField("isMobile", BooleanType(), True),
    StructField("isMonitor", BooleanType(), True),

    StructField("instruments", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])), True),

    StructField("sensors", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("parameter", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("units", StringType(), True),
            StructField("displayName", StringType(), True)
        ]))
    ])), True),

    StructField("coordinates", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]), True),

    StructField("licenses", licenses_schema, True),

    StructField("bounds", ArrayType(DoubleType()), True),

    StructField("distance", DoubleType(), True),

    StructField("datetimeFirst", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ]), True),

    StructField("datetimeLast", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ]), True)
])


sensor_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),

    StructField("locationId", IntegerType(), True),

    StructField("parameter", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("units", StringType(), True),
        StructField("displayName", StringType(), True)
    ]), True),

    StructField("coordinates", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]), True),
    StructField("coverage", StringType(), True),

    # Other optional fields
    StructField("isMobile", BooleanType(), True),
    StructField("isMonitor", BooleanType(), True),
    StructField("datetimeFirst", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ]), True),
    StructField("datetimeLast", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ]), True)
])

measurements_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("parameter", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("datetime_utc", TimestampType(), True),
    StructField("datetime_local", TimestampType(), True)
])

