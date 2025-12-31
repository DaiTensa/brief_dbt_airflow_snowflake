with trips as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
)

select
    pickup_day,
    pickup_month,
    cast(pickup_datetime as date) as date,
    
    count(*) as trip_count,
    avg(trip_distance) as avg_distance,
    sum(total_amount) as total_revenue,
    avg(fare_amount) as avg_fare,
    avg(duration_minutes) as avg_duration_minutes,
    avg(avg_speed_mph) as avg_speed_mph

from trips
group by 1, 2, 3
order by date desc