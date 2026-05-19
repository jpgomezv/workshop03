-- Raw events table: stores original Kafka messages with processing status
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id SERIAL PRIMARY KEY,
    country VARCHAR(100),
    year INT,
    gdp FLOAT,
    family FLOAT,
    health FLOAT,
    freedom FLOAT,
    generosity FLOAT,
    corruption FLOAT,
    actual_happiness_score FLOAT,
    raw_payload JSONB,
    processing_status VARCHAR(50) NOT NULL DEFAULT 'RECEIVED',
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Country dimension
CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

-- Date dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT UNIQUE NOT NULL
);

-- Raw event dimension (links fact to raw event metadata)
CREATE TABLE IF NOT EXISTS dim_raw_event (
    raw_event_id INTEGER PRIMARY KEY REFERENCES raw_happiness_events(raw_event_id),
    received_at TIMESTAMP,
    processing_status VARCHAR(50)
);

-- Predictions fact table
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id SERIAL PRIMARY KEY,
    raw_event_id INT NOT NULL REFERENCES raw_happiness_events(raw_event_id),
    country_id INT NOT NULL REFERENCES dim_country(country_id),
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    actual_score FLOAT,
    predicted_score FLOAT,
    prediction_error FLOAT,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
