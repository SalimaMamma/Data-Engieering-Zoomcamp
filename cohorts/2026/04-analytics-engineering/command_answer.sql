

SELECT COUNT(*) FROM `zoomcamp-taxi.dbt_smamma.fct_monthly_zone_revenue`


SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM `zoomcamp-taxi.dbt_smamma.fct_monthly_zone_revenue`
WHERE 
    service_type = 'Green'
    AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;

SELECT SUM(total_monthly_trips) as total_trips
FROM `zoomcamp-taxi.dbt_smamma.fct_monthly_zone_revenue`
WHERE 
    service_type = 'Green'
    AND EXTRACT(YEAR FROM revenue_month) = 2019
    AND EXTRACT(MONTH FROM revenue_month) = 10;


SELECT COUNT(*) FROM `zoomcamp-taxi.dbt_smamma.stg_fhv_tripdata` 