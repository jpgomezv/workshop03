-- 1. Average prediction error
SELECT
    AVG(ABS(prediction_error)) AS avg_absolute_error,
    AVG(prediction_error) AS avg_error
FROM fact_predictions;

-- 2. Predictions by country
SELECT
    c.country_name,
    COUNT(*) AS prediction_count,
    AVG(fp.prediction_error) AS avg_error,
    AVG(fp.actual_score) AS avg_actual,
    AVG(fp.predicted_score) AS avg_predicted
FROM fact_predictions fp
JOIN dim_country c ON fp.country_id = c.country_id
GROUP BY c.country_name
ORDER BY prediction_count DESC;

-- 3. Predicted vs actual score
SELECT
    c.country_name,
    d.year,
    fp.actual_score,
    fp.predicted_score,
    fp.prediction_error
FROM fact_predictions fp
JOIN dim_country c ON fp.country_id = c.country_id
JOIN dim_date d ON fp.date_id = d.date_id
ORDER BY d.year, c.country_name;

-- 4. Prediction trends over time
SELECT
    d.year,
    COUNT(*) AS total_predictions,
    AVG(fp.actual_score) AS avg_actual,
    AVG(fp.predicted_score) AS avg_predicted,
    AVG(ABS(fp.prediction_error)) AS avg_abs_error
FROM fact_predictions fp
JOIN dim_date d ON fp.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;
