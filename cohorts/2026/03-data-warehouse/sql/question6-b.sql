SELECT DISTINCT VendorID
FROM `zoomcamp-taxi.taxi_dataset.yellow_taxi_optimized` 
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';