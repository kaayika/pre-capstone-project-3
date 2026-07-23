DROP TABLE IF EXISTS bronze.taxi_trips;

CREATE TABLE bronze.taxi_trips (
    vendor_id DOUBLE PRECISION,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    ratecode_id DOUBLE PRECISION,
    store_and_fwd_flag TEXT,
    pu_location_id DOUBLE PRECISION,
    do_location_id DOUBLE PRECISION,
    payment_type DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    cbd_congestion_fee DOUBLE PRECISION
);
