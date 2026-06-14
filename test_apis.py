import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Test EIA
eia_key = os.getenv("EIA_API_KEY")
eia_url = f"https://api.eia.gov/v2/electricity/retail-sales/data/?api_key={eia_key}&frequency=monthly&data[0]=price&sort[0][column]=period&sort[0][direction]=desc&length=5"
eia_response = requests.get(eia_url)
print("EIA Status:", eia_response.status_code)
print("EIA Response:", eia_response.json())

# Test NOAA
noaa_token = os.getenv("NOAA_API_TOKEN")
noaa_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&stationid=GHCND:USW00014922&startdate=2024-01-01&enddate=2024-01-05&datatypeid=TMAX&units=standard"
noaa_response = requests.get(noaa_url, headers={"token": noaa_token})
print("NOAA Status:", noaa_response.status_code)
print("NOAA Response:", noaa_response.json())

# Test Carbon Intensity
ci_response = requests.get("https://api.carbonintensity.org.uk/intensity")
print("Carbon Intensity Status:", ci_response.status_code)