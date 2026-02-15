{{ config(materialized='view') }}

WITH fhv_data AS (
    SELECT
        -- identifiers
        dispatching_base_num,
        affiliated_base_number,
        
        -- timestamps
        CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        
        -- location IDs
        CAST(pulocationid AS INTEGER) AS pickup_location_id,
        CAST(dolocationid AS INTEGER) AS dropoff_location_id,
        
        sr_flag
        
    FROM {{ source('raw_fhv', 'raw_fhv_tripdata_2019') }}
    WHERE dispatching_base_num IS NOT NULL
)

SELECT * FROM fhv_data
