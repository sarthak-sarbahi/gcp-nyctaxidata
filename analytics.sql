CREATE OR REPLACE TABLE `sunlit-vortex-394519.nyctaxitrnsfmdata.nyc_taxi_odl` AS (
SELECT 
f.trip_id,
f.vendor_id,
p.pickup_datetime,
d.drop_datetime,
f.passenger_count,
f.trip_distance,
py.payment_type,
py.fare_amount,
py.extra,
py.mta_tax,
py.tip_amount,
py.tolls_amount,
py.improvement_surcharge,
py.total_amount
FROM 
`sunlit-vortex-394519.nyctaxitrnsfmdata.fact_table` f  
JOIN `sunlit-vortex-394519.nyctaxitrnsfmdata.pickup_dim` p ON p.pickup_id = f.pickup_id
JOIN `sunlit-vortex-394519.nyctaxitrnsfmdata.drop_dim` d ON d.drop_id = f.drop_id
JOIN `sunlit-vortex-394519.nyctaxitrnsfmdata.payment_dim` py ON py.payment_id = f.payment_id
);