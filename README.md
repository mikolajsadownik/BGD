 Raport projektu BGD – Mini Data Pipeline

## 1. Cel projektu
Celem projektu było zaprojektowanie i uruchomienie prostego, ale kompletnego pipeline’u danych typu **Bronze → Silver → ML**, z wykorzystaniem nowoczesnych narzędzi Big Data i MLOps.

Projekt demonstruje:
- orkiestrację zadań (Airflow),
- przetwarzanie danych (Spark),
- kontrolę jakości danych (walidacja),
- trenowanie i rejestrowanie modeli ML (MLflow).

---

## 2. Architektura rozwiązania

Pipeline składa się z następujących komponentów:

- **Apache Airflow** – orkiestracja całego procesu (DAG)
- **Apache Spark** – transformacja danych (Bronze → Silver)
- **Pandas + walidacja jakości danych** – kontrola poprawności danych Silver
- **MLflow** – trenowanie, logowanie metryk i artefaktów modelu
- **Docker Compose** – uruchomienie całego środowiska

Schemat logiczny:
1. Generacja danych (Bronze)
2. Transformacja danych (Silver)
3. Walidacja jakości danych
4. Trening modelu ML i zapis do MLflow

---

## 3. Etapy pipeline’u

### 3.1 Bronze – generacja danych
Na etapie Bronze generowane są surowe dane zamówień:
- format: CSV
- lokalizacja: `data/bronze/orders.csv`

Dane zawierają m.in.:
- `order_id`
- `country`
- `amount`
- `order_date`

---

### 3.2 Silver – transformacja danych
Dane Bronze są przetwarzane za pomocą Apache Spark:
- czyszczenie danych,
- standaryzacja typów,
- zapis w formacie Parquet.

Wynik:
- lokalizacja: `data/silver/orders_silver.parquet/`
- format kolumnowy Parquet (wydajny analitycznie)

---

### 3.3 Walidacja jakości danych
Na danych Silver wykonywana jest walidacja jakościowa.

Sprawdzane reguły:
- tabela zawiera co najmniej 1 rekord,
- kolumna `order_id` istnieje, nie zawiera NULL i duplikatów,
- kolumna `amount` nie zawiera NULL i wartości ujemnych.

Wynik walidacji:
- zapis do pliku `gx/validation_result.json`
- pipeline przerywa się w przypadku błędów jakości

---

### 3.4 Trening modelu i MLflow
Na zwalidowanych danych trenowany jest prosty model regresyjny (scikit-learn).

Rejestrowane w MLflow:
- metryki: RMSE, MAE, R²
- artefakt modelu (`model/`)

MLflow dostępny jest pod adresem:
- http://localhost:5000

---

## 4. Rezultaty

- Pipeline wykonuje się w całości automatycznie
- Dane przechodzą pełny cykl Bronze → Silver → ML
- Model jest zapisany i możliwy do ponownego użycia
- Jakość danych jest jawnie kontrolowana

Przykładowe metryki modelu:
- RMSE ≈ 184
- MAE ≈ 158
- R² < 0 (model bazowy, demonstracyjny)

---

