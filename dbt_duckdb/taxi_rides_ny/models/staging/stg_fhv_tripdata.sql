with source as (
    select * from {{source("raw_data", "fhv_tripdata")}}
), renamed as (
    select
        dispatching_base_num,
        cast(pickup_datetime as timestamp),
        cast(dropOff_datetime as timestamp) as dropoff_datetime,
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,
        cast(SR_Flag as integer) as shared_ride_flag,
        cast(Affiliated_base_number as string) as affiliated_base_number

        from source
        where dispatching_base_num is not null
)

select count(*) from renamed