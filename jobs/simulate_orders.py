from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import random
import csv


def main():
    root = Path(__file__).resolve().parents[1]
    bronze_path = root / "data" / "bronze" / "orders.csv"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    random.seed(42)
    today = datetime.now().date()

    rows = []
    for i in range(1, 51):
        country = random.choice(["PL", "DE", "FR"])  # prosto
        amount = round(random.uniform(10, 500), 2)
        order_date = (today - timedelta(days=random.randint(0, 30))).isoformat()
        rows.append([i, country, amount, order_date])

    # Dodaj 2 "brudne" wiersze, zeby bylo co poprawiac w DQ
    rows.append([999, "PL", "", today.isoformat()])
    rows.append([1000, "XX", -10, today.isoformat()])

    with bronze_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "country", "amount", "order_date"])
        w.writerows(rows)

    print(f"OK: zapisano {len(rows)} wierszy do {bronze_path}")


if __name__ == "__main__":
    main()
