import json
import sys
from pathlib import Path

import pandas as pd


def main():
    root = Path("/opt/airflow")
    silver_path = root / "data" / "silver" / "orders_silver.parquet"
    out_dir = root / "gx"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not silver_path.exists():
        result = {
            "success": False,
            "errors": [f"Silver parquet not found: {str(silver_path)}"],
        }
        (out_dir / "validation_result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(result)
        sys.exit(1)

    # Spark zapisuje parquet jako folder; pandas odczyta to przez pyarrow
    df = pd.read_parquet(silver_path)

    errors = []

    # 1) min 1 row
    if len(df) < 1:
        errors.append("Table has 0 rows")

    # 2) kolumny
    required_cols = ["order_id", "amount"]
    for c in required_cols:
        if c not in df.columns:
            errors.append(f"Missing column: {c}")

    # Dalsze checki tylko jeśli kolumny istnieją
    if "order_id" in df.columns:
        if df["order_id"].isna().any():
            errors.append("order_id contains nulls")
        if df["order_id"].duplicated().any():
            errors.append("order_id contains duplicates")

    if "amount" in df.columns:
        if df["amount"].isna().any():
            errors.append("amount contains nulls")
        # amount >= 0 (tylko dla wartości liczbowych)
        try:
            if (df["amount"] < 0).any():
                errors.append("amount contains negative values")
        except Exception:
            errors.append("amount is not comparable (non-numeric?)")

    success = len(errors) == 0

    result = {
        "success": success,
        "row_count": int(len(df)),
        "columns": list(map(str, df.columns)),
        "errors": errors,
    }

    (out_dir / "validation_result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(result)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
