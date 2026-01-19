from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import numpy as np

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


def main():
    root = Path(__file__).resolve().parents[1]
    silver_dir = root / "data" / "silver" / "orders_silver.parquet"
    if not silver_dir.exists():
        raise FileNotFoundError(f"Silver not found: {silver_dir}")

    # MLflow inside docker-compose: usluga nazywa sie 'mlflow'
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("BGD-Orders-Ridge")

    df = pd.read_parquet(silver_dir)
    if df.empty:
        raise RuntimeError("Silver is empty - nothing to train")

    # Proste cechy
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["dayofweek"] = df["order_date"].dt.dayofweek.fillna(0).astype(int)

    X = df[["country", "dayofweek"]]
    y = df["amount"].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

    preprocess = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), ["country"]),
            ("num", "passthrough", ["dayofweek"]),
        ]
    )

    model = Ridge(alpha=1.0, random_state=42)
    pipe = Pipeline(steps=[("preprocess", preprocess), ("model", model)])

    with mlflow.start_run():
        mlflow.log_param("model", "Ridge")
        mlflow.log_param("alpha", 1.0)
        mlflow.log_param("test_size", 0.25)

        pipe.fit(X_train, y_train)
        pred = pipe.predict(X_test)

        rmse = float(mean_squared_error(y_test, pred, squared=False))
        mae = float(mean_absolute_error(y_test, pred))
        r2 = float(r2_score(y_test, pred))

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)

        mlflow.sklearn.log_model(pipe, artifact_path="model", input_example=X_test.head(5))

        print(f"OK: trained + logged to MLflow ({mlflow_uri}). RMSE={rmse:.3f} MAE={mae:.3f} R2={r2:.3f}")


if __name__ == "__main__":
    main()
