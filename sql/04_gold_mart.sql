DROP TABLE IF EXISTS gold.daily_taxi_summary;

CREATE TABLE gold.daily_taxi_summary AS
SELECT
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS average_revenue,
    SUM(trip_distance) AS total_trip_distance,
    AVG(trip_distance) AS average_trip_distance
FROM silver.taxi_trips_clean
GROUP BY
    DATE(pickup_datetime)
ORDER BY
    trip_date;
