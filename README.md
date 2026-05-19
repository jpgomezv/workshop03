# Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning

**Course:** ETL (G01)  
**Institution:** Faculty of Engineering and Basic Sciences — Data Engineering & AI

## Project Description

This project implements a streaming ETL pipeline that ingests raw World Happiness Report data (2015–2019), streams it through Apache Kafka, validates each event, and generates real-time happiness score predictions using a pre-trained Random Forest regression model. Results are stored in a PostgreSQL star-schema database and visualized through a Metabase dashboard.

The project demonstrates the transition from traditional batch ETL to event-driven streaming architectures, combining data profiling, machine learning, and real-time inference.

## Architecture

The system is split into two independent processes:

### Offline (Batch) — Part A
```
Raw CSV Files (2015–2019)
    ↓
EDA + Schema Harmonization
    ↓
Feature Engineering
    ↓
Model Training (Random Forest)
    ↓
Save model.pkl
```

### Streaming (Real-time) — Part B
```
Raw CSV Files (2015–2019)
    ↓
Kafka Producer → streams each row as JSON
    ↓
Kafka Topic: happiness-predictions
    ↓
Kafka Consumer
    ├── Store raw event in PostgreSQL
    ├── Validate schema & values
    ├── Load model.pkl
    ├── Generate prediction
    └── Store in star schema (dimensions + fact)
    ↓
Metabase Dashboard → queries PostgreSQL directly
```

### Infrastructure
```
Docker Compose
├── Zookeeper (port 2181)
├── Kafka Broker (port 9092)
├── PostgreSQL 16 (port 5433)
└── Metabase (port 3000)
```

## Folder Structure

```
workshop03/
├── data/
│   ├── raw/                  # Original CSVs (2015–2019)
│   ├── processed/            # Harmonized dataset (output of EDA)
│   └── streaming/            # (reserved)
├── notebooks/
│   ├── eda.ipynb             # Part A Steps 1-2: EDA + Cleaning
│   └── model_training.ipynb  # Part A Steps 3-4: Features + Training
├── kafka/
│   ├── producer.py           # Part B Step 5: Streams raw data to Kafka
│   └── consumer.py           # Part B Step 6 + Part C: Validates, predicts, stores
├── models/
│   └── model.pkl             # Serialized Random Forest model
├── sql/
│   ├── create_tables.sql     # Star schema DDL
│   └── kpis.sql              # Dashboard KPI queries
├── dashboards/               # Dashboard screenshots
├── docker-compose.yml        # Zookeeper + Kafka + PostgreSQL + Metabase
├── pyproject.toml            # uv project configuration
├── requirements.txt          # Pinned dependencies
└── README.md                 # This file
```

## Data Cleaning Decisions

The five CSV files (2015–2019) use **different column naming conventions** and contain **different sets of columns**:

| Issue | Decision | Justification |
|---|---|---|
| Column name variations (e.g., `Happiness Score` vs `Score` vs `Happiness.Score`) | Mapped to unified names (e.g., `happiness_score`) | 9 core concepts exist across all years, only naming differs |
| Confidence measure columns (`Standard Error`, `Lower/Upper Confidence Interval`, `Whisker.high/low`) | Dropped | Each year uses a different metric — cannot be unified |
| `Region` column (only in 2015–2016) | Dropped, continent added via `country_converter` | Missing from 2017–2019, replaced with continent for geographical analysis |
| `Dystopia Residual` column (only 2015–2017) | Dropped | Not present in 2018–2019 |
| 1 missing value in `corruption` (UAE 2018) | Filled with column mean (0.1254) in the harmonized dataset | Single missing value; mean is a reasonable imputation for this low-variance field |
| Country name variants (e.g., "Hong Kong" vs "Hong Kong S.A.R., China") | Preserved as-is | Country is not a model feature; name variants don't affect prediction |
| Zero values in health/gdp/freedom (5–6 rows) | Kept as genuine values | Represent genuinely low indicators for impoverished nations (e.g., Somalia GDP≈0) |

## Feature Engineering Decisions

| Decision | Choice | Justification |
|---|---|---|
| Target | `happiness_score` | Workshop requirement |
| Features (6) | `gdp, family, health, freedom, generosity, corruption` | Matches Kafka JSON format exactly; no continent lookup needed in consumer |
| Excluded: `happiness_rank` | Target leakage | Correlation of -0.992 with target |
| Excluded: `country` | 170 categories | Would explode features with one-hot encoding; not a causal predictor |
| Excluded: `continent` | Optional but excluded | Adding continent improves R² by +0.04 but requires country→continent lookup in the consumer, adding pipeline complexity |
| Excluded: `year` | Temporal metadata | Not a causal driver of happiness |
| Scaling | `StandardScaler` applied after train/test split | Features are in 0–2 range; scaling ensures consistent handling |

Model used: **Random Forest Regressor** (100 estimators, max_depth=10, random_state=42)  
Train/test split: **70% / 30%**  
Metrics on test set: **R² ≈ 0.78, MAE ≈ 0.41, RMSE ≈ 0.52**

## Kafka Pipeline

### Producer (`kafka/producer.py`)

1. Reads raw CSV files from `data/raw/` (2015–2019)
2. Maps columns to a unified schema matching the workshop-specified JSON format
3. Converts numpy types to native Python (NaN → `null` in JSON)
4. Streams each row one-by-one with 0.5s delay to Kafka topic `happiness-predictions`

**JSON format sent:**
```json
{
  "country": "Colombia",
  "year": 2019,
  "gdp": 1.2,
  "family": 0.8,
  "health": 0.9,
  "freedom": 0.6,
  "generosity": 0.3,
  "corruption": 0.1,
  "actual_happiness_score": 6.2
}
```

### Consumer (`kafka/consumer.py`)

Per-event processing flow:

1. **Receive** message from Kafka topic
2. **Store raw event** in `raw_happiness_events` (status = `RECEIVED`)
3. **Populate** `dim_raw_event` dimension
4. **Validate** event against required schema:
   - Missing fields → `INVALID_SCHEMA`
   - Wrong data types → `INVALID_SCHEMA`
   - Null/NaN in numeric fields → `INVALID_VALUES`
5. **On failure** → update status, commit, skip prediction (never crashes)
6. **Extract features** in model order: `[gdp, family, health, freedom, generosity, corruption]`
7. **Predict** using `model.pkl`
8. **On prediction error** → mark `PREDICTION_ERROR`, skip
9. **UPSERT** `dim_country` and `dim_date`
10. **INSERT** into `fact_predictions` with `raw_event_id` link
11. **Mark** as `VALID`

### Validation & Error Handling

| Status | When | Action |
|---|---|---|
| `RECEIVED` | Initial state | Event stored, not yet validated |
| `VALID` | All checks passed | Prediction stored in fact table |
| `INVALID_SCHEMA` | Missing field / wrong type | Stored in raw table, skipped |
| `INVALID_VALUES` | Null in required field | Stored in raw table, skipped |
| `PREDICTION_ERROR` | Model failed to predict | Stored in raw table, skipped |

**Real result from full run:** 782 raw events → 781 VALID predictions + 1 INVALID_VALUES (UAE 2018 — missing corruption value)

## Database Schema (Star Schema)

### Tables

| Table | Type | Rows (full run) | Purpose |
|---|---|---|---|
| `raw_happiness_events` | Raw | 782 | Original Kafka messages + processing status |
| `dim_raw_event` | Dimension | 782 | Links fact to raw event metadata (id, status, received_at) |
| `dim_country` | Dimension | 170 | Country dimension |
| `dim_date` | Dimension | 5 | Year dimension |
| `fact_predictions` | Fact | 781 | Prediction results linked to all dimensions |

### Entity Relationship

```
fact_predictions
├── raw_event_id  →  raw_happiness_events.raw_event_id
├── country_id    →  dim_country.country_id
├── date_id       →  dim_date.date_id
└── (actual_score, predicted_score, prediction_error, prediction_timestamp)

dim_raw_event
└── raw_event_id  →  raw_happiness_events.raw_event_id
```

## Dashboard (Metabase)

Metabase is included in the Docker Compose stack and connects directly to the PostgreSQL database.

### Setup Steps

1. Start all containers: `docker compose up -d`
2. Open http://localhost:3000 in your browser
3. Complete the Metabase initial setup (create admin account)
4. Add the PostgreSQL database as a data source:
   - Host: `postgres` (Docker internal network)
   - Port: `5432`
   - Database: `happiness_predictions`
   - Username: `workshop`
   - Password: `workshop`

### KPI Queries

The following SQL queries are available in `sql/kpis.sql`:

**1. Average Prediction Error**
```sql
SELECT AVG(ABS(prediction_error)) AS mae
FROM fact_predictions;
```
→ Display as a **single number** card

**2. Predictions by Country**
```sql
SELECT c.country_name AS country,
       COUNT(*) AS predictions,
       AVG(ABS(fp.prediction_error)) AS avg_abs_error,
       AVG(fp.actual_score) AS avg_actual,
       AVG(fp.predicted_score) AS avg_predicted
FROM fact_predictions fp
JOIN dim_country c ON fp.country_id = c.country_id
GROUP BY c.country_name
ORDER BY predictions DESC;
```
→ Display as a **bar chart** or **table**

**3. Predicted vs Actual Score**
```sql
SELECT c.country_name AS country,
       d.year,
       fp.actual_score,
       fp.predicted_score,
       fp.prediction_error
FROM fact_predictions fp
JOIN dim_country c ON fp.country_id = c.country_id
JOIN dim_date d ON fp.date_id = d.date_id
ORDER BY fp.actual_score DESC;
```
→ Display as a **scatter plot** (actual vs predicted)

**4. Prediction Trends Over Time**
```sql
SELECT d.year,
       AVG(fp.actual_score) AS avg_actual,
       AVG(fp.predicted_score) AS avg_predicted,
       AVG(ABS(fp.prediction_error)) AS avg_abs_error
FROM fact_predictions fp
JOIN dim_date d ON fp.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;
```
→ Display as a **line chart** (dual series: actual + predicted)

### Dashboard Screenshots

Place screenshots of the completed Metabase dashboard in the `dashboards/` folder.

---

## Execution Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

### 1. Clone and install dependencies
```bash
cd workshop03
uv sync
```

### 2. Start infrastructure
```bash
docker compose up -d
```

### 3. Create Kafka topic
```bash
docker exec kafka kafka-topics --create \
  --topic happiness-predictions \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### 4. Initialize database tables
```bash
docker exec -i postgres psql -U workshop -d happiness_predictions < sql/create_tables.sql
```

### 5. Run Part A (optional — model is pre-trained)
```bash
uv run jupyter notebook notebooks/eda.ipynb
uv run jupyter notebook notebooks/model_training.ipynb
```

### 6. Run the streaming pipeline
```bash
# Terminal 1 — Consumer
uv run python kafka/consumer.py

# Terminal 2 — Producer  
uv run python kafka/producer.py
```

### 7. Set up the dashboard
Open http://localhost:3000 and configure Metabase as described in the Dashboard section above.

### 8. Stop infrastructure
```bash
docker compose down
```

---

## Technical Requirements

| Component | Technology |
|---|---|
| Streaming | Apache Kafka (Confluent 7.6.1) |
| Database | PostgreSQL 16 |
| ML Model | Random Forest Regressor (scikit-learn) |
| Dashboard | Metabase |
| Producer/Consumer | Python (kafka-python-ng) |
| DB Connection | SQLAlchemy + psycopg2 |
| Containerization | Docker Compose |
| Package Management | uv |

## Evaluation Criteria

| Criteria | Weight |
|---|---|
| Data Integration & Cleaning | 1.0 |
| Feature Engineering | 0.5 |
| ML Pipeline | 0.5 |
| Kafka Producer | 0.5 |
| Kafka Consumer | 0.5 |
| Event Validation | 0.5 |
| Database Design & Loading | 0.5 |
| Dashboard & KPIs | 0.5 |
| Documentation & Reproducibility | 0.5 |
