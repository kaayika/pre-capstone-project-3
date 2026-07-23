DROP TABLE IF EXISTS silver.taxi_trips_clean;

CREATE TABLE silver.taxi_trips_clean AS
SELECT DISTINCT
    *
FROM bronze.taxi_trips
WHERE
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime >= pickup_datetime
    AND trip_distance IS NOT NULL
    AND total_amount IS NOT NULL
    AND trip_distance >= 0
    AND total_amount >= 0;
