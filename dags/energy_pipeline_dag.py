from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time
import os

DATABRICKS_HOST = "https://dbc-27921f89-0ace.cloud.databricks.com"
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


def run_databricks_notebook(notebook_path, **kwargs):
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    task_key = notebook_path.split('/')[-1]

    run_payload = {
        "run_name": f"airflow_run_{task_key}",
        "tasks": [
            {
                "task_key": task_key,
                "notebook_task": {"notebook_path": notebook_path}
            }
        ]
    }

    # Retry on rate limit (429)
    max_retries = 5
    submit_response = None
    for attempt in range(max_retries):
        submit_response = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
            headers=headers,
            json=run_payload
        )
        if submit_response.status_code == 429:
            wait_time = 30 * (attempt + 1)
            print(f"Rate limited. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
            time.sleep(wait_time)
            continue
        break

    print("Response status:", submit_response.status_code)
    print("Response body:", submit_response.text)
    submit_response.raise_for_status()
    run_id = submit_response.json()["run_id"]
    print(f"Submitted run_id: {run_id}")

    # Poll until completion
    while True:
        status_response = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id}
        )
        status_response.raise_for_status()
        state = status_response.json()["state"]
        life_cycle_state = state.get("life_cycle_state")
        print(f"Run {run_id} status: {life_cycle_state}")

        if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result_state = state.get("result_state")
            if result_state != "SUCCESS":
                raise Exception(f"Notebook run failed: {state}")
            print(f"Run {run_id} completed successfully")
            break

        time.sleep(15)


default_args = {
    "owner": "sam_weller",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="energy_intelligence_pipeline",
    default_args=default_args,
    description="End-to-end Bronze/Silver/Gold pipeline for energy, weather, and pricing data",
    schedule="@daily",
    start_date=datetime(2026, 6, 20),
    catchup=False,
    tags=["energy", "databricks", "medallion"],
) as dag:

    bronze_eia_prices = PythonOperator(
        task_id="bronze_eia_prices",
        python_callable=run_databricks_notebook,
        op_kwargs={"notebook_path": "/Workspace/Users/samoweller@comcast.net/Bronze/bronze_eia_prices"},
    )

    bronze_eia_generation = PythonOperator(
        task_id="bronze_eia_generation",
        python_callable=run_databricks_notebook,
        op_kwargs={"notebook_path": "/Workspace/Users/samoweller@comcast.net/Bronze/bronze_eia_generation"},
    )

    bronze_noaa = PythonOperator(
        task_id="bronze_noaa_ingestion",
        python_callable=run_databricks_notebook,
        op_kwargs={"notebook_path": "/Workspace/Users/samoweller@comcast.net/Bronze/bronze_noaa_ingestion"},
    )

    silver_eia_prices = PythonOperator(
        task_id="silver_eia_electricity_prices",
        python_callable=run_databricks_notebook,
        op_kwargs={"notebook_path": "/Workspace/Users/samoweller@comcast.net/Silver/silver_eia_electricity_prices"},
    )

    silver_eia_generation = PythonOperator(
        task_id="silver_eia_generation",
        python_callable=run_databricks_notebook,
        op_kwargs={"notebook_path": "/Workspace/Users/samoweller@comcast.net/Silver/silver_eia_generation"},
    )

    silver_noaa = PythonOperator(
        task_id="silver_noaa_weather",
        python_callable=run_databricks_notebook,
        op_kwargs={"notebook_path": "/Workspace/Users/samoweller@comcast.net/Silver/silver_noaa_weather"},
    )

    gold_summary = PythonOperator(
        task_id="gold_monthly_state_summary",
        python_callable=run_databricks_notebook,
        op_kwargs={"notebook_path": "/Workspace/Users/samoweller@comcast.net/Gold/gold_monthly_state_summary"},
    )

    bronze_eia_prices >> silver_eia_prices
    bronze_eia_generation >> silver_eia_generation
    bronze_noaa >> silver_noaa
    [silver_eia_prices, silver_eia_generation, silver_noaa] >> gold_summary