# Energy Intelligence Pipeline

An AI powered energy assistant built for non technical users to explore electricity pricing, weather, and power generation across all 50 U.S. states just by asking plain English questions.

Ask something like *"Is there a correlation between average temperature and residential electricity prices in 2023?"* and the agent writes the SQL code to query the data, runs it against the data, calculates the actual answer, explains it in plain language, and decides on its own whether a table, line chart, bar chart, or scatter plot best fits the question.

Underneath it, a custom data pipeline ingests and transforms the data through a medallion (Bronze/Silver/Gold) architecture in Databricks, orchestrated daily with Apache Airflow which feeds the clean, structured dataset the AI agent actually queries.

## Architecture

```
EIA API ─┐
NOAA API ─┼─→ Bronze (raw) → Silver (cleaned) → Gold (joined, business-ready) → Text-to-SQL Agent
          ┘
```

**Bronze layer** — raw data ingested as is from three sources:
- **EIA** electricity retail prices (all sectors, monthly, 2020–2026, all states).
- **EIA** electricity generation by fuel type (filtered to natural gas, coal, nuclear, solar, wind, and hydro).
- **NOAA** daily weather observations (temperature, precipitation, snowfall, wind speed) from one station per state.

**Silver layer** — cleaned, typed, and standardized:
- All three sources cleaned and standardized: proper decimal types instead of strings, consistent column naming, dates parsed into year/month for easy joining, irrelevant columns dropped, and a few additional calculated fields added.

**Gold layer** — Join out cleaned data sets into one final table
- Join all of our tables together to get one row per state per month, combining overall and sector-level electricity prices, weather aggregates, and weather-relevant generation by fuel type. Built to avoid join explosion.

**Orchestration** — an Airflow DAG runs the pipeline daily: Bronze ingests first, then Silver transforms once its Bronze source is done, then Gold builds once all three Silver tables are ready.

**Agent layer** — a Streamlit app sends the user's question and the Gold table schema to Claude, which generates SQL. The query runs against Databricks via SQL warehouse, and a second Claude call decides whether the result is better shown as a table, line chart, or bar chart (rendered with Plotly).

## Tech stack

- **Ingestion:** Python, `requests`, EIA Open Data API, NOAA Climate Data Online API
- **Processing:** Databricks, Apache Spark (PySpark), Delta Lake
- **Orchestration:** Apache Airflow (via Astro CLI), Docker
- **Agent:** Claude (Anthropic API), Streamlit, Plotly, Databricks SQL Connector

## Project structure

```
energy-intelligence-pipeline/
├── dags/
│   └── energy_pipeline_dag.py       # Airflow DAG
├── notebooks/
│   ├── Bronze/
│   │   ├── bronze_eia_prices.py
│   │   ├── bronze_eia_generation.py
│   │   └── bronze_noaa_weather.py
│   ├── Silver/
│   │   ├── silver_eia_prices.py
│   │   ├── silver_eia_generation.py
│   │   └── silver_noaa_weather.py
│   └── Gold/
│       └── gold_monthly_state_summary.py
├── agents/
│   └── app.py                       # Streamlit text-to-SQL agent
├── Dockerfile
├── packages.txt
├── requirements.txt
└── .env.example
```

## Running it

This project runs against a Databricks workspace (Free Edition works) and requires API keys for EIA, NOAA, Databricks, and Anthropic. The notebooks under `notebooks/` are exported Databricks source files — meant to be run inside a Databricks workspace with Spark available, not as standalone local scripts.

1. Clone the repo and copy `.env.example` to `.env`, filling in your own keys
2. Upload the Bronze/Silver/Gold notebooks to your Databricks workspace and run them in order (or let the Airflow DAG handle it)
3. To run the orchestration locally: `astro dev start` (requires Docker Desktop and the Astro CLI)
4. To run the agent: `pip install -r requirements.txt` then `streamlit run agents/app.py`

## Data sources

- [EIA Open Data API](https://www.eia.gov/opendata/) — electricity retail prices and generation by fuel type
- [NOAA Climate Data Online](https://www.ncei.noaa.gov/cdo-web/) — daily weather observations
