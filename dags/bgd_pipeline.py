from __future__ import annotations

from datetime import datetime, timedelta
import os
import subprocess
from typing import Optional, Dict

from docker.types import Mount

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


# -----------------------
# Config
# -----------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# PROJECT_DIR przychodzi z docker-compose (Windows absolute path)
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow")

APP = "/app"
HOST_JOBS = f"{PROJECT_DIR}/jobs"
HOST_DATA = f"{PROJECT_DIR}/data"

JOBS = f"{APP}/jobs"
DATA = f"{APP}/data"

BRONZE = f"{DATA}/bronze/orders.csv"
SILVER = f"{DATA}/silver/orders_silver.parquet"


# -----------------------
# Helpers
# -----------------------
def run_py(script_name: str, env: Optional[Dict[str, str]] = None):
    """Run python script inside Airflow container."""
    script_path = f"/opt/airflow/jobs/{script_name}"
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    subprocess.check_call(["python", script_path], env=merged_env)


# -----------------------
# DAG
# -----------------------
with DAG(
    dag_id="bgd_mini_pipeline",
    description="BGD mini pipeline: bronze -> silver -> GX -> MinIO -> MLflow",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bgd"],
) as dag:

    # 1) Generate bronze CSV
    simulate_bronze = PythonOperator(
        task_id="simulate_bronze",
        python_callable=run_py,
        op_args=["simulate_orders.py"],
    )

    # 2) Spark bronze -> silver
    spark_bronze_to_silver = DockerOperator(
        task_id="spark_bronze_to_silver",
        image="apache/spark:3.5.1-python3",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=HOST_JOBS, target=JOBS, type="bind"),
            Mount(source=HOST_DATA, target=DATA, type="bind"),
        ],
        command=(
            "bash -lc "
            f"\"/opt/spark/bin/spark-submit --master local[*] "
            f"{JOBS}/transform_silver.py {BRONZE} {SILVER}\""
        ),
    )

    # 3) Great Expectations validation (wynik: /opt/airflow/gx/validation_result.json)
    gx_validate_silver = PythonOperator(
        task_id="gx_validate_silver",
        python_callable=run_py,
        op_args=["validate_silver_gx.py"],
    )

    # 4) Upload artefaktów do MinIO (Bronze + Silver + GX result)
    upload_to_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=run_py,
        op_args=["upload_to_minio.py"],
        op_kwargs={
            "env": {
                "MINIO_ENDPOINT": "http://minio:9000",
                "MINIO_ACCESS_KEY": "minioadmin",
                "MINIO_SECRET_KEY": "minioadmin123",
                "MINIO_BUCKET": "bgd",
                "BRONZE_CSV": "/opt/airflow/data/bronze/orders.csv",
                "SILVER_DIR": "/opt/airflow/data/silver/orders_silver.parquet",
                "GX_JSON": "/opt/airflow/gx/validation_result.json",
            }
        },
    )

    # 5) Train model + MLflow
    train_model_mlflow = PythonOperator(
        task_id="train_model_mlflow",
        python_callable=run_py,
        op_args=["train_mlflow.py"],
        op_kwargs={
            "env": {
                "MLFLOW_TRACKING_URI": "http://mlflow:5000",
            }
        },
    )

    simulate_bronze >> spark_bronze_to_silver >> gx_validate_silver >> upload_to_minio >> train_model_mlflow
