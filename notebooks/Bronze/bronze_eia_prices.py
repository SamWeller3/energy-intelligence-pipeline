import os
import requests
from dotenv import load_dotenv
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

load_dotenv()
EIA_API_KEY = os.getenv("EIA_API_KEY")


def fetch_eia_prices():
    url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
    all_data = []
    offset = 0
    page_size = 5000

    while True:
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "monthly",
            "data[0]": "price",
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "start": "2020-01",
            "length": page_size,
            "offset": offset
        }
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        batch = data['response']['data']

        if not batch:
            break

        all_data.extend(batch)
        print(f"Fetched {len(batch)} records (offset {offset}), total so far: {len(all_data)}")

        if len(batch) < page_size:
            break

        offset += page_size

    return all_data


raw_data = fetch_eia_prices()
print(f"\nTotal fetched: {len(raw_data)} records")
print("Earliest period:", raw_data[0]['period'])
print("Latest period:", raw_data[-1]['period'])

# Convert to Spark DataFrame
rows = [Row(**record) for record in raw_data]
df = spark.createDataFrame(rows)

df = df.withColumn("ingested_at", current_timestamp()) \
       .withColumn("source", lit("EIA"))

print(f"\nRow count: {df.count()}")
df.printSchema()
display(df.limit(10))

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_eia_electricity_prices")

print(f"Written {df.count()} records to bronze_eia_electricity_prices")