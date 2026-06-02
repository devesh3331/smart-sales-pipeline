from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import subprocess
import sys

# ── Default arguments ──────────────────────────────────────
default_args = {
    "owner": "devesh",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ─────────────────────────────────────────
dag = DAG(
    dag_id="smart_sales_pipeline",
    default_args=default_args,
    description="End-to-end sales ETL pipeline",
    schedule="0 6 * * *",  # runs daily at 6am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "sales", "data-engineering"]
)

# ── Task functions ─────────────────────────────────────────
# PROJECT_PATH = r"C:\Users\deves\Projects\smart-sales-pipeline"
PROJECT_PATH = "/usr/local/airflow"

# def task_ingestion():
#     print("Starting ingestion...")
#     raw_path = os.path.join(PROJECT_PATH, "data", "raw")
#     landing_path = os.path.join(PROJECT_PATH, "data", "landing")
#     os.makedirs(landing_path, exist_ok=True)

#     files = [f for f in os.listdir(raw_path) if f.endswith(".csv")]
#     for file in files:
#         src = os.path.join(raw_path, file)
#         dst = os.path.join(landing_path, file)
#         import shutil
#         shutil.copy(src, dst)
#         print(f"Ingested: {file}")

#     print(f"Ingestion complete. {len(files)} files processed.")

def task_ingestion():
    print("Starting ingestion...")
    raw_path = os.path.join(PROJECT_PATH, "dags", "data", "raw")
    landing_path = os.path.join(PROJECT_PATH, "dags", "data", "landing")
    os.makedirs(landing_path, exist_ok=True)

    import shutil
    files = [f for f in os.listdir(raw_path) if f.endswith(".csv")]
    for file in files:
        src = os.path.join(raw_path, file)
        dst = os.path.join(landing_path, file)
        shutil.copy(src, dst)
        print(f"Ingested: {file}")

    print(f"Ingestion complete. {len(files)} files processed.")

def task_transform():
    print("Starting transformation...")
    script = os.path.join(PROJECT_PATH, "transform", "transform.py")
    result = subprocess.run(
        [sys.executable, script],
        capture_output=True, text=True,
        cwd=PROJECT_PATH
    )
    if result.returncode != 0:
        raise Exception(f"Transform failed:\n{result.stderr}")
    print(result.stdout)
    print("Transformation complete.")

def task_data_quality():
    print("Starting data quality checks...")
    script = os.path.join(PROJECT_PATH, "quality", "data_quality.py")
    result = subprocess.run(
        [sys.executable, script],
        capture_output=True, text=True,
        cwd=PROJECT_PATH
    )
    if result.returncode != 0:
        raise Exception(f"Data quality failed:\n{result.stderr}")
    print(result.stdout)

    # Fail the DAG if quality checks found errors
    if "FAILED" in result.stdout:
        raise Exception("Data quality checks failed — pipeline halted.")
    print("Data quality complete.")

def task_notify_success():
    print("=" * 40)
    print("Pipeline completed successfully!")
    print(f"Run time: {datetime.now()}")
    print("All tasks: ingestion → transform → quality → done")
    print("=" * 40)

# ── Task definitions ───────────────────────────────────────
t1_ingest = PythonOperator(
    task_id="ingestion",
    python_callable=task_ingestion,
    dag=dag,
)

t2_transform = PythonOperator(
    task_id="transformation",
    python_callable=task_transform,
    dag=dag,
)

t3_quality = PythonOperator(
    task_id="data_quality",
    python_callable=task_data_quality,
    dag=dag,
)

t4_notify = PythonOperator(
    task_id="notify_success",
    python_callable=task_notify_success,
    dag=dag,
)

# ── Task dependencies (the pipeline order) ────────────────
t1_ingest >> t2_transform >> t3_quality >> t4_notify