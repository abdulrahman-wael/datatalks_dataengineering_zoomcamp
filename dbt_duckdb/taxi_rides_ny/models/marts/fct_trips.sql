with trips_unioned as (
    select * from {{ref("int_trips_unioned")}}
), 
trips as (
    select 
        {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime', 'pickup_location_id']) }} as trip_id,
        vendor_id ,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,

        pickup_datetime,
        dropoff_datetime,

        store_and_fwd_flag,
        passenger_count,
        trip_distance,

        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type
        from trips_unioned
)

select count(payment_type) from trips
where payment_type is null
-- total: 44459136