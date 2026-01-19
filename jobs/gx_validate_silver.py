from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import great_expectations as gx


def main():
    root = Path(__file__).resolve().parents[1]
    silver_dir = root / "data" / "silver" / "orders_silver.parquet"
    report_path = root / "docs" / "gx_report.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    # Spark zapisuje parquet jako katalog
    if not silver_dir.exists():
        raise FileNotFoundError(f"Brak silver: {silver_dir}")

    df = pd.read_parquet(silver_dir)

    context = gx.get_context()  # Ephemeral wystarczy
    ds = context.data_sources.add_pandas(name="pandas")
    asset = ds.add_dataframe_asset(name="orders_silver")
    batch_def = asset.add_batch_definition_whole_dataframe("full")

    suite = gx.ExpectationSuite(name="silver_suite")
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="country", value_set=["PL","DE","FR"]))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0, max_value=100000))

    context.suites.add(suite)

    validation = gx.ValidationDefinition(data=batch_def, suite=suite, name="silver_validation")

    # podajemy dataframe do batcha
    results = validation.run(batch_parameters={"dataframe": df})

    # Prosty HTML report (wymaganie projektu: raport HTML)
    summary = {
        "success": results.success,
        "statistics": results.statistics,
    }

    html = f"""<!doctype html>
<html lang='pl'>
<head><meta charset='utf-8'><title>Great Expectations Report</title></head>
<body>
<h1>Great Expectations – Silver Validation</h1>
<p><b>Success:</b> {summary['success']}</p>
<pre>{json.dumps(summary, indent=2, ensure_ascii=False)}</pre>
</body>
</html>"""

    report_path.write_text(html, encoding="utf-8")
    print("OK: GX report ->", report_path)
    print("Success:", results.success)

    # fail fast dla Airflow
    if not results.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
