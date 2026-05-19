"""
Kafka Consumer — Step 6 (+ Part C Steps 7-8)
Consumes raw events from Kafka, validates them, runs ML inference, and
stores raw events + predictions in PostgreSQL following a star schema.

The producer sends raw CSV data (uncleaned) as JSON. The consumer must:
  1. Receive from Kafka
  2. Store raw event in `raw_happiness_events` (status=RECEIVED)
  3. Validate schema & values
  4. On failure → mark INVALID_SCHEMA / INVALID_VALUES, skip prediction
  5. Extract features in model order
  6. Generate prediction via model.pkl
  7. On prediction error → mark PREDICTION_ERROR, skip
  8. UPSERT `dim_country` and `dim_date`
  9. INSERT into `fact_predictions`
  10. Populate `dim_raw_event` dimension
 11. Mark raw event as VALID

Validation categories:
  - INVALID_SCHEMA: missing required field, wrong data type
  - INVALID_VALUES: null/NaN in a numeric feature, or out-of-range values
"""

import json
import logging
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path

import joblib
from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore", message="X does not have valid feature names")

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p",
)
logger = logging.getLogger(__name__)

TOPIC = os.getenv("KAFKA_TOPIC", "happiness-predictions")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = "happiness-consumer-group"

DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
    f"/{os.getenv('POSTGRES_DB')}"
)

MODEL_PATH = Path(__file__).resolve().parents[1] / "models" / "model.pkl"

# Must match the order used during model training
FEATURE_ORDER = ["gdp", "family", "health", "freedom", "generosity", "corruption"]

# Required fields and their expected Python types (from JSON deserialization)
REQUIRED_FIELDS = {
    "country": str,
    "year": int,
    "gdp": (int, float),
    "family": (int, float),
    "health": (int, float),
    "freedom": (int, float),
    "generosity": (int, float),
    "corruption": (int, float),
    "actual_happiness_score": (int, float),
}

# Numeric fields that must be non-null for prediction
NUMERIC_REQUIRED_FIELDS = [
    "gdp", "family", "health", "freedom",
    "generosity", "corruption", "actual_happiness_score",
]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_event(event: dict) -> tuple[bool, str | None, str]:
    """
    Validate an incoming event against the required schema.
    Returns (is_valid, error_type, detail) where error_type is None on success
    or one of: 'INVALID_SCHEMA', 'INVALID_VALUES'.
    """
    # Check for missing fields
    for field in REQUIRED_FIELDS:
        if field not in event:
            return False, "INVALID_SCHEMA", f"missing field '{field}'"

    # Check data types
    for field, expected_type in REQUIRED_FIELDS.items():
        value = event[field]
        if value is None:
            continue  # will be caught in the next loop
        if not isinstance(value, expected_type):
            return False, "INVALID_SCHEMA", f"field '{field}' has wrong type: {type(value).__name__}"

    # Check for null/None in numeric fields (NaN from raw CSV becomes None in JSON)
    for field in NUMERIC_REQUIRED_FIELDS:
        value = event[field]
        if value is None:
            return False, "INVALID_VALUES", f"field '{field}' is null"

    return True, None, ""


def extract_features(event: dict) -> list:
    """Extract features in the exact order the model expects."""
    return [[float(event[field]) for field in FEATURE_ORDER]]


def insert_raw_event(conn, event: dict) -> int:
    """Store the original Kafka message and return the raw_event_id."""
    result = conn.execute(
        text(
            """
            INSERT INTO raw_happiness_events (
                country, year, gdp, family, health, freedom,
                generosity, corruption, actual_happiness_score,
                raw_payload, processing_status, received_at
            )
            VALUES (
                :country, :year, :gdp, :family, :health, :freedom,
                :generosity, :corruption, :actual_happiness_score,
                :raw_payload, 'RECEIVED', :received_at
            )
            RETURNING raw_event_id
            """
        ),
        {
            "country": event.get("country"),
            "year": event.get("year"),
            "gdp": event.get("gdp"),
            "family": event.get("family"),
            "health": event.get("health"),
            "freedom": event.get("freedom"),
            "generosity": event.get("generosity"),
            "corruption": event.get("corruption"),
            "actual_happiness_score": event.get("actual_happiness_score"),
            "raw_payload": json.dumps(event, ensure_ascii=False),
            "received_at": now_utc(),
        },
    )
    return result.scalar_one()


def update_raw_status(conn, raw_event_id: int, status: str) -> None:
    """Update the processing status of a raw event."""
    conn.execute(
        text(
            "UPDATE raw_happiness_events SET processing_status = :status WHERE raw_event_id = :raw_event_id"
        ),
        {"status": status, "raw_event_id": raw_event_id},
    )
    # Also update the dim_raw_event dimension
    conn.execute(
        text(
            "UPDATE dim_raw_event SET processing_status = :status WHERE raw_event_id = :raw_event_id"
        ),
        {"status": status, "raw_event_id": raw_event_id},
    )


def insert_dim_raw_event(conn, raw_event_id: int) -> None:
    """Populate the raw event dimension (links fact to raw event metadata)."""
    conn.execute(
        text(
            """
            INSERT INTO dim_raw_event (raw_event_id, received_at, processing_status)
            VALUES (:raw_event_id, :received_at, 'RECEIVED')
            ON CONFLICT (raw_event_id) DO NOTHING
            """
        ),
        {"raw_event_id": raw_event_id, "received_at": now_utc()},
    )


def upsert_dim_country(conn, country_name: str) -> int:
    """Insert or get country dimension id."""
    result = conn.execute(
        text(
            """
            INSERT INTO dim_country (country_name)
            VALUES (:country_name)
            ON CONFLICT (country_name) DO UPDATE SET country_name = EXCLUDED.country_name
            RETURNING country_id
            """
        ),
        {"country_name": country_name},
    )
    return result.scalar_one()


def upsert_dim_date(conn, year: int) -> int:
    """Insert or get date dimension id."""
    result = conn.execute(
        text(
            """
            INSERT INTO dim_date (year)
            VALUES (:year)
            ON CONFLICT (year) DO UPDATE SET year = EXCLUDED.year
            RETURNING date_id
            """
        ),
        {"year": year},
    )
    return result.scalar_one()


def insert_fact_prediction(
    conn,
    raw_event_id: int,
    country_id: int,
    date_id: int,
    actual_score: float,
    predicted_score: float,
) -> None:
    """Insert a prediction result into the fact table."""
    prediction_error = actual_score - predicted_score
    conn.execute(
        text(
            """
            INSERT INTO fact_predictions (
                raw_event_id, country_id, date_id,
                actual_score, predicted_score, prediction_error,
                prediction_timestamp
            )
            VALUES (
                :raw_event_id, :country_id, :date_id,
                :actual_score, :predicted_score, :prediction_error,
                :prediction_timestamp
            )
            """
        ),
        {
            "raw_event_id": raw_event_id,
            "country_id": country_id,
            "date_id": date_id,
            "actual_score": actual_score,
            "predicted_score": predicted_score,
            "prediction_error": prediction_error,
            "prediction_timestamp": now_utc(),
        },
    )


def consume_events() -> None:
    logger.info("Loading model from %s", MODEL_PATH)
    model = joblib.load(MODEL_PATH)
    logger.info("Model loaded (type=%s, features=%d)", type(model).__name__, len(FEATURE_ORDER))

    engine = create_engine(DB_URL)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info("Consumer started. Listening on topic '%s'...", TOPIC)
    logger.info("Press Ctrl+C to stop.")

    processed = 0
    invalid_schema = 0
    invalid_values = 0
    prediction_errors = 0

    try:
        with engine.connect() as conn:
            for msg in consumer:
                event = msg.value
                country = event.get("country", "?")
                year = event.get("year", "?")
                logger.info("Received: %s (%s)", country, year)

                # Step 1: Store raw event immediately (before any validation)
                try:
                    raw_event_id = insert_raw_event(conn, event)
                    insert_dim_raw_event(conn, raw_event_id)
                    conn.commit()
                except Exception as exc:
                    logger.error("Failed to insert raw event: %s", exc)
                    conn.rollback()
                    consumer.commit()
                    continue

                # Step 2: Validate event schema and values
                is_valid, error_type, detail = validate_event(event)
                if not is_valid:
                    update_raw_status(conn, raw_event_id, error_type)
                    conn.commit()
                    if error_type == "INVALID_SCHEMA":
                        invalid_schema += 1
                    else:
                        invalid_values += 1
                    logger.warning("REJECTED (%s): %s — %s", error_type, country, detail)
                    consumer.commit()
                    continue

                # Step 3: Extract features and predict
                try:
                    features = extract_features(event)
                    predicted_score = float(model.predict(features)[0])
                except Exception as exc:
                    update_raw_status(conn, raw_event_id, "PREDICTION_ERROR")
                    conn.commit()
                    prediction_errors += 1
                    logger.error("Prediction failed for %s (%s): %s", country, year, exc)
                    consumer.commit()
                    continue

                # Step 4: Store dimensions and prediction result
                try:
                    country_id = upsert_dim_country(conn, event["country"])
                    date_id = upsert_dim_date(conn, event["year"])
                    insert_fact_prediction(
                        conn,
                        raw_event_id,
                        country_id,
                        date_id,
                        float(event["actual_happiness_score"]),
                        predicted_score,
                    )
                except Exception as exc:
                    update_raw_status(conn, raw_event_id, "PREDICTION_ERROR")
                    conn.commit()
                    prediction_errors += 1
                    logger.error("Failed to store prediction for %s: %s", country, exc)
                    consumer.commit()
                    continue

                # Step 5: Mark as valid
                update_raw_status(conn, raw_event_id, "VALID")
                conn.commit()
                processed += 1
                actual = event["actual_happiness_score"]
                error = actual - predicted_score
                logger.info(
                    "VALID | %s (%s) | actual=%.3f predicted=%.3f error=%+.3f",
                    country, year, actual, predicted_score, error,
                )
                consumer.commit()

    except KeyboardInterrupt:
        logger.warning("Consumer stopped by user.")
    finally:
        consumer.close()
        engine.dispose()
        logger.info(
            "Summary → processed: %d | invalid_schema: %d | invalid_values: %d | prediction_errors: %d",
            processed, invalid_schema, invalid_values, prediction_errors,
        )


if __name__ == "__main__":
    consume_events()
