# ingest/01_openaq_api_batch.py

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession, Row

def fetch_and_write_to_bronze(spark, url, table_name, api_key):
    headers = {"X-API-Key": api_key}
    print(f"📡 Fetching data: {url}")

    response = requests.get(url, headers=headers)

    # If 404 — return gracefully instead of crashing
    if response.status_code == 404:
        print(f"⚠️ 404 Not Found: {url}")
        return False

    response.raise_for_status()

    data_list = response.json().get('results', [])
    ingestion_ts = datetime.utcnow()

    if not data_list:
        print(f"⚠️ No results returned for: {url}")
        return False

    rows = [
        Row(
            raw_json=json.dumps(item),
            ingestion_ts=ingestion_ts,
            source_url=url
        ) for item in data_list
    ]

    df = spark.createDataFrame(rows)

    df.write.format("delta").mode("append").saveAsTable(table_name)

    print(f"✅ Wrote {len(rows)} records → {table_name}")
    return True



def run_openaq_ingestion(spark, country_code="TN", locations_limit=100):
    print("🔄 Starting OpenAQ ingestion...")

    OPENAQ_API_KEY = "bb3f7808b0e6125c0b8e0c8c15980baeb7c19c92f0a956d06e3caa08f7234476"   # ADD YOUR KEY

    # -----------------------------
    # 1️⃣ FETCH LOCATIONS
    # -----------------------------
    url_locations = (
        f"https://api.openaq.org/v3/locations?"
        f"country={country_code}&limit={locations_limit}"
    )

    fetch_and_write_to_bronze(
        spark=spark,
        url=url_locations,
        table_name="bronze.openaq_locations",
        api_key=OPENAQ_API_KEY
    )

    # Read locations
    df_locations = spark.read.table("bronze.openaq_locations")

    # Extract location → sensors
    locations = [
        json.loads(r.raw_json) for r in df_locations.collect()
    ]

    print(f"📍 Found {len(locations)} locations")

    # -----------------------------
    # 2️⃣ EXTRACT SENSOR IDs
    # -----------------------------
    sensor_ids = []
    for loc in locations:
        for sensor in loc.get("sensors", []):
            sensor_ids.append(sensor["id"])

    print(f"📟 Found {len(sensor_ids)} sensors")

    # -----------------------------
    # 3️⃣ FETCH MEASUREMENTS PER SENSOR
    # -----------------------------
    count = 0
    for sensor_id in sensor_ids:
        url_measurements = (
            f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements?limit=100"
        )

        if fetch_and_write_to_bronze(
            spark=spark,
            url=url_measurements,
            table_name="bronze.openaq_measurements",
            api_key=OPENAQ_API_KEY
        ):
            count += 1

    print(f"🎉 Finished fetching measurements for {count} sensors!")
