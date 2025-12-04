import time
import requests
from pyspark.sql import SparkSession, Row
from delta.tables import DeltaTable
from pyspark.sql.types import *
from pyspark.sql import functions as F
from src.data_ingestion.models import location_schema
spark = SparkSession.builder.getOrCreate()


# --- CONFIG ---
OPENAQ_API_KEY = "bb3f7808b0e6125c0b8e0c8c15980baeb7c19c92f0a956d06e3caa08f7234476"
LOCATION_IDS = [2162939,2162982,2162940,2162946,2162945,2162942,2162943,2162944,2162947,2162981,2162980,2162989,2162965]
BASE_URL = "https://api.openaq.org/v3"
import math
import random

session = requests.Session()
session.headers.update({"X-API-Key": OPENAQ_API_KEY, "Accept": "application/json"})

def safe_get(url, params=None, max_retries=6):
    attempt = 0
    while attempt < max_retries:
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                reset = resp.headers.get("x-ratelimit-reset")
                remaining = resp.headers.get("x-ratelimit-remaining")
                if reset and reset.isdigit():
                    sleep_for = int(reset) + 1
                else:
                    sleep_for = min(60, 2 ** attempt + random.random())
                print(f"429 received. Sleeping {sleep_for}s (attempt {attempt+1}/{max_retries})")
                time.sleep(sleep_for)
                attempt += 1
                continue

            resp.raise_for_status()
            try:
                remaining = int(resp.headers.get("x-ratelimit-remaining", "1000"))
                if remaining < 5:
                    time.sleep(1 + random.random())
            except Exception:
                pass
            return resp.json()
        except requests.RequestException as e:
            sleep_for = min(60, (2 ** attempt) + random.random())
            print(f"Request error: {e}. Sleeping {sleep_for}s (attempt {attempt+1}/{max_retries})")
            time.sleep(sleep_for)
            attempt += 1
    raise RuntimeError(f"Failed GET {url} after {max_retries} attempts")


def fetch_air_quality_locations(location_ids=LOCATION_IDS):
    all_rows = []
    for loc_id in location_ids:
        url = f"{BASE_URL}/locations/{loc_id}"
        try:
            json_data = safe_get(url)
            results = json_data.get("results", [])
            if isinstance(results, dict):
                results = [results]
            for r in results:
                r.setdefault("licenses", [])
                if isinstance(r["licenses"], dict):
                    r["licenses"] = [r["licenses"]]
                r.setdefault("sensors", [])
                r.setdefault("instruments", [])
                r.setdefault("bounds", [])
            all_rows.extend([Row(**r) for r in results])
        except Exception as e:
            print(f" Error fetching location {loc_id}: {e}")

    if not all_rows:
        print(" No location data found.")
        return
    
    df_new = spark.createDataFrame(all_rows, schema=location_schema)
    df_new = (
        df_new.withColumn("datetimeFirst_utc", F.to_timestamp(F.col("datetimeFirst.utc")))
            .withColumn("datetimeLast_utc",  F.to_timestamp(F.col("datetimeLast.utc")))
            .drop("datetimeFirst", "datetimeLast")
    )
    table_name = "bronze.air_quality_locations_bronze"
    if spark.catalog.tableExists(table_name):
        print(f"Merging updates into {table_name}...")
        delta_table = DeltaTable.forName(spark, table_name)
        (delta_table.alias("old")
            .merge(df_new.alias("new"), "old.id = new.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f" Merged {len(all_rows)} locations into {table_name}")
    else:
        print(f"Creating new table {table_name}...")
        df_new.write.format("delta").saveAsTable(table_name)
        print(f" Created {table_name} with {len(all_rows)} locations")


