with int_trips_unioned as (
    select *    
    from {{ref('stg_yellow_tripdata')}}
)

select * from int_trips_unioned