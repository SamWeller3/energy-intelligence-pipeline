import os
import time
import requests
from dotenv import load_dotenv
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

load_dotenv()
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN")

STATE_STATIONS = {
    "AL": "GHCND:USW00013876",
    "AK": "GHCND:USW00026451",
    "AZ": "GHCND:USW00023183",
    "AR": "GHCND:USW00013963",
    "CA": "GHCND:USW00023174",
    "CO": "GHCND:USW00003017",
    "CT": "GHCND:USW00014740",
    "DE": "GHCND:USW00013781",
    "FL": "GHCND:USW00012839",
    "GA": "GHCND:USW00013874",
    "HI": "GHCND:USW00022521",
    "ID": "GHCND:USW00024131",
    "IL": "GHCND:USW00094846",
    "IN": "GHCND:USW00093819",
    "IA": "GHCND:USW00094910",
    "KS": "GHCND:USW00013996",
    "KY": "GHCND:USW00093820",
    "LA": "GHCND:USW00012916",
    "ME": "GHCND:USW00014764",
    "MD": "GHCND:USW00093721",
    "MA": "GHCND:USW00014739",
    "MI": "GHCND:USW00094847",
    "MN": "GHCND:USW00014922",
    "MS": "GHCND:USW00013882",
    "MO": "GHCND:USW00013994",
    "MT": "GHCND:USW00024143",
    "NE": "GHCND:USW00014939",
    "NV": "GHCND:USW00023169",
    "NH": "GHCND:USW00014745",
    "NJ": "GHCND:USW00014734",
    "NM": "GHCND:USW00023050",
    "NY": "GHCND:USW00014732",
    "NC": "GHCND:USW00013722",
    "ND": "GHCND:USW00024011",
    "OH": "GHCND:USW00014820",
    "OK": "GHCND:USW00013967",
    "OR": "GHCND:USW00024229",
    "PA": "GHCND:USW00014737",
    "RI": "GHCND:USW00014765",
    "SC": "GHCND:USW00013880",
    "SD": "GHCND:USW00024090",
    "TN": "GHCND:USW00013897",
    "TX": "GHCND:USW00012960",
    "UT": "GHCND:USW00024127",
    "VT": "GHCND:USW00014742",
    "VA": "GHCND:USW00013741",
    "WA": "GHCND:USW00024233",
    "WV": "GHCND:USW00003853",
    "WI": "GHCND:USW00014898",
    "WY": "GHCND:USW00024018"
}


def fetch_noaa_weather_all_states(start_year, end_year):
    url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    all_results = []
    headers = {"token": NOAA_API_TOKEN}

    for year in range(start_year, end_year + 1):
        if year == 2026:
            half_years = [("01-01", "06-14")]
        else:
            half_years = [("01-01", "06-30"), ("07-01", "12-31")]

        for half_start, half_end in half_years:
            for state, station_id in STATE_STATIONS.items():
                params = {
                    "datasetid": "GHCND",
                    "stationid": station_id,
                    "startdate": f"{year}-{half_start}",
                    "enddate": f"{year}-{half_end}",
                    "units": "standard",
                    "limit": 1000,
                    "datatypeid": ["TMAX", "TMIN", "PRCP", "AWND", "SNOW", "TAVG"]
                }

                try:
                    response = requests.get(url, params=params, headers=headers, timeout=30)
                    if response.status_code == 200:
                        results = response.json().get("results", [])
                        for record in results:
                            record["state"] = state
                        all_results.extend(results)
                        print(f"{state} {year} H{'1' if half_start == '01-01' else '2'}: {len(results)} records")
                    else:
                        print(f"{state} {year}: failed - {response.status_code}")
                except Exception as e:
                    print(f"{state} {year}: error - {e}")

                time.sleep(0.2)

    return all_results


raw_weather = fetch_noaa_weather_all_states(2020, 2026)
print(f"\nTotal records fetched: {len(raw_weather)}")

# Convert to Spark DataFrame
rows = [Row(**record) for record in raw_weather]
df = spark.createDataFrame(rows)

df = df.withColumn("ingested_at", current_timestamp()) \
       .withColumn("source", lit("NOAA"))

print(f"\nRow count: {df.count()}")
df.printSchema()
display(df.limit(10))

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_noaa_weather")

print(f"Written {df.count()} records to bronze_noaa_weather")