import os
import requests
from dotenv import load_dotenv
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

load_dotenv()
EIA_API_KEY = os.getenv("EIA_API_KEY")

url = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"

page_size = 5000
offset = 0
all_data = []
seen_first_records = set()

# Only the fuel types and sector we actually use downstream
RELEVANT_FUEL_TYPES = ["NG", "COL", "NUC", "SUN", "WND", "HYC"]
RELEVANT_SECTOR = "99"

while True:
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "monthly",
        "data[0]": "generation",
        "facets[fueltypeid][]": RELEVANT_FUEL_TYPES,
        "facets[sectorid][]": RELEVANT_SECTOR,
        "start": "2020-01",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": offset,
        "length": page_size,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    response_json = response.json()
    page = response_json["response"]["data"]

    if not page:
        print("No more records returned. Finished.")
        break

    first_key = str(page[0])
    if first_key in seen_first_records:
        print("Detected duplicate page. Stopping pagination.")
        break
    seen_first_records.add(first_key)

    all_data.extend(page)

    total_available = response_json["response"].get("total")

    print(
        f"Offset={offset} | "
        f"Fetched={len(page)} | "
        f"Running Total={len(all_data)} | "
        f"API Total={total_available}"
    )

    if total_available is not None and len(all_data) >= int(total_available):
        print("Reached API reported total.")
        break

    if len(page) < page_size:
        print("Last page reached.")
        break

    offset += page_size

print(f"\nFinal record count: {len(all_data)}")

rows = [Row(**record) for record in all_data]
df = spark.createDataFrame(rows)

df = df.withColumn("ingested_at", current_timestamp()) \
       .withColumn("source", lit("EIA_GENERATION"))

print(f"\nRow count: {df.count()}")
df.printSchema()
display(df.limit(10))

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_eia_generation")

print(f"Written {df.count()} records to bronze_eia_generation")