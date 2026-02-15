CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-taxi.taxi_dataset.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://yellow-taxi-salima-2/yellow_tripdata_2024-01.parquet',
    'gs://yellow-taxi-salima-2/yellow_tripdata_2024-02.parquet',
    'gs://yellow-taxi-salima-2/yellow_tripdata_2024-03.parquet',
    'gs://yellow-taxi-salima-2/yellow_tripdata_2024-04.parquet',
    'gs://yellow-taxi-salima-2/yellow_tripdata_2024-05.parquet',
    'gs://yellow-taxi-salima-2/yellow_tripdata_2024-06.parquet'
  ]
);
