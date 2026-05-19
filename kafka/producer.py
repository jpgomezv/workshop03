"""
Kafka Producer — Step 5
Reads raw CSV files (2015-2019), maps columns to a unified schema,
and streams each row as a JSON event to Kafka.

The raw CSVs have different schemas — the producer maps them to the
workshop-specified JSON format but does NOT clean or transform values.
NaN values are kept as-is (they become `null` in JSON).
"""

import json
import time
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p",
)
logger = logging.getLogger(__name__)

TOPIC = "happiness-predictions"
BOOTSTRAP_SERVERS = "localhost:9092"
DELAY_SECONDS = 0.1
DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw"

# Map all raw column names to the unified JSON field names
COLUMN_MAPPING = {
    # Country
    'Country': 'country',
    'Country or region': 'country',

    # Happiness Score → actual_happiness_score in JSON
    'Happiness Score': 'actual_happiness_score',
    'Happiness.Score': 'actual_happiness_score',
    'Score': 'actual_happiness_score',

    # GDP / Economy
    'Economy (GDP per Capita)': 'gdp',
    'Economy..GDP.per.Capita.': 'gdp',
    'GDP per capita': 'gdp',

    # Family / Social Support
    'Family': 'family',
    'Social support': 'family',

    # Health / Life Expectancy
    'Health (Life Expectancy)': 'health',
    'Health..Life.Expectancy.': 'health',
    'Healthy life expectancy': 'health',

    # Freedom
    'Freedom': 'freedom',
    'Freedom to make life choices': 'freedom',

    # Generosity (consistent across years)
    'Generosity': 'generosity',

    # Corruption / Trust
    'Trust (Government Corruption)': 'corruption',
    'Trust..Government.Corruption.': 'corruption',
    'Perceptions of corruption': 'corruption',
}

# Fields in the JSON message (must match workshop spec + consumer expectations)
STREAMING_FIELDS = [
    'country', 'year', 'gdp', 'family', 'health',
    'freedom', 'generosity', 'corruption', 'actual_happiness_score',
]


def json_serializer(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def publish_events() -> None:
    logger.info("Reading raw CSVs from %s", DATA_PATH)

    raw_dfs = []
    for year in [2015, 2016, 2017, 2018, 2019]:
        file_path = DATA_PATH / f"{year}.csv"
        df = pd.read_csv(file_path)
        df = df.rename(columns=COLUMN_MAPPING)
        df['year'] = year
        # Keep only the streaming fields
        available_cols = [c for c in STREAMING_FIELDS if c in df.columns]
        df = df[available_cols]
        raw_dfs.append(df)
        logger.info("  %s: %d rows, columns=%s", year, len(df), available_cols)

    # Concatenate all years — raw values, no cleaning
    df_raw = pd.concat(raw_dfs, ignore_index=True)

    logger.info("Total records to stream: %d", len(df_raw))
    logger.info("Sample record: %s", json.dumps(df_raw.iloc[0].to_dict(), ensure_ascii=False))

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
    )

    sent = 0
    try:
        for _, row in df_raw.iterrows():
            event = row.to_dict()
            # Convert numpy types → native Python, and NaN → None
            clean_event = {}
            for k, v in event.items():
                if pd.isna(v):
                    clean_event[k] = None
                elif isinstance(v, (np.integer,)):
                    clean_event[k] = int(v)
                elif isinstance(v, (np.floating,)):
                    clean_event[k] = float(v)
                else:
                    clean_event[k] = v
            event = clean_event
            key = f"{event.get('country', 'unknown')}_{event.get('year', 0)}"
            producer.send(TOPIC, key=key.encode("utf-8"), value=event)
            sent += 1
            logger.info(
                "Sent %d/%d: %s (%s)",
                sent, len(df_raw), event.get('country'), event.get('year'),
            )
            time.sleep(DELAY_SECONDS)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info("Published %d events to topic '%s'.", sent, TOPIC)


if __name__ == "__main__":
    publish_events()
