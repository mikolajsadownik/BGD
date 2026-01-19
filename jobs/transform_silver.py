from __future__ import annotations

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def main(bronze_csv: str, silver_parquet: str):
    spark = (
        SparkSession.builder
        .appName("bronze_to_silver")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(bronze_csv)
    )

    # --- proste czyszczenie (Silver) ---
    # 1) order_date -> date
    df = df.withColumn("order_date", to_date(col("order_date")))

    # 2) filtrujemy braki i ujemne kwoty
    df = df.filter(col("amount").isNotNull())
    df = df.filter(col("amount") >= 0)

    # 3) tylko znane kraje
    df = df.filter(col("country").isin(["PL", "DE", "FR"]))

    # zapis do parquet (wygodne do ML)
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(silver_parquet)
    )

    print(f"OK: zapisano Silver do {silver_parquet}")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: transform_silver.py <bronze_csv> <silver_parquet>")
        sys.exit(2)

    main(sys.argv[1], sys.argv[2])
