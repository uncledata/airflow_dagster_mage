select
    coalesce(vendor_name, 'UNDEFINED') as vendor_name,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    coalesce(rate_name, 'UNDEFINED') as rate_name,
    store_and_fwd_flag,
    coalesce(pu_zone.borough, 'UNDEFINED') as pickup_borough,
    coalesce(pu_zone.zone, 'UNDEFINED') as pickup_zone,
    coalesce(pu_zone.service_zone, 'UNDEFINED') as pickup_service_zone,
    coalesce(do_zone.borough, 'UNDEFINED') as dropoff_borough,
    coalesce(do_zone.zone, 'UNDEFINED') as dropoff_zone,
    coalesce(do_zone.service_zone, 'UNDEFINED') as dropoff_service_zone,
    coalesce(payment_type_name, 'UNDEFINED') as payment_type_name,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    row_belongs_to_period
from {{ source("yellow_taxi", "clean") }} as clean
left join
    {{ ref("dim_payments") }} on dim_payments.payment_type_id = clean.payment_type
left join {{ ref("dim_rates") }} on dim_rates.rate_code_id = clean.rate_code_id
left join
    {{ ref("dim_taxi_zones") }} as pu_zone
    on pu_zone.location_id = clean.pu_location_id
left join
    {{ ref("dim_taxi_zones") }} as do_zone
    on do_zone.location_id = clean.do_location_id
left join {{ ref("dim_vendor") }} on dim_vendor.vendor_id = clean.vendor_id


