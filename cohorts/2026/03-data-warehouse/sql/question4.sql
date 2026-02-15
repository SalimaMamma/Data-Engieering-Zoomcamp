SELECT COUNT(*) as zero_fare_count FROM `zoomcamp-taxi.taxi_dataset.yellow_taxi_table`
WHERE fare_amount = 0;