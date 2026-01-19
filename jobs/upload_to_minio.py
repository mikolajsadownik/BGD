"""
upload_to_minio.py
- Uploaduje artefakty pipeline'u do MinIO (S3):
  * Bronze CSV
  * Silver Parquet (katalog)
  * wynik walidacji GX (JSON)

Uruchomienie (w kontenerze Airflow):
  python /opt/airflow/jobs/upload_to_minio.py
"""

from __future__ import annotations

import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError


# --- Konfiguracja MinIO ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET = os.getenv("MINIO_BUCKET", "bgd")


# --- Ścieżki w kontenerze Airflow ---
BRONZE_CSV = Path(os.getenv("BRONZE_CSV", "/opt/airflow/data/bronze/orders.csv"))
SILVER_DIR = Path(os.getenv("SILVER_DIR", "/opt/airflow/data/silver/orders_silver.parquet"))
GX_JSON = Path(os.getenv("GX_JSON", "/opt/airflow/gx/validation_result.json"))


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(client):
    try:
        client.head_bucket(Bucket=BUCKET)
        print(f"Bucket exists: {BUCKET}")
    except ClientError:
        print(f"Creating bucket: {BUCKET}")
        client.create_bucket(Bucket=BUCKET)


def upload_file(client, local_path: Path, key: str):
    if not local_path.exists():
        raise FileNotFoundError(f"Nie ma pliku: {local_path}")
    client.upload_file(str(local_path), BUCKET, key)
    print(f"Uploaded: {local_path} -> s3://{BUCKET}/{key}")


def upload_dir(client, local_dir: Path, prefix: str):
    if not local_dir.exists():
        raise FileNotFoundError(f"Nie ma katalogu: {local_dir}")
    files = [p for p in local_dir.rglob("*") if p.is_file()]
    if not files:
        raise FileNotFoundError(f"Katalog jest pusty: {local_dir}")
    for p in files:
        rel = p.relative_to(local_dir).as_posix()
        key = f"{prefix.rstrip('/')}/{rel}"
        client.upload_file(str(p), BUCKET, key)
    print(f"Uploaded dir: {local_dir} -> s3://{BUCKET}/{prefix}/ (files: {len(files)})")


def main():
    client = s3_client()
    ensure_bucket(client)

    # 1) Bronze
    upload_file(client, BRONZE_CSV, "bronze/orders.csv")

    # 2) Silver (parquet katalog)
    upload_dir(client, SILVER_DIR, "silver/orders_silver.parquet")

    # 3) GX result
    upload_file(client, GX_JSON, "gx/validation_result.json")

    print("OK: upload do MinIO zakonczony sukcesem.")


if __name__ == "__main__":
    main()
