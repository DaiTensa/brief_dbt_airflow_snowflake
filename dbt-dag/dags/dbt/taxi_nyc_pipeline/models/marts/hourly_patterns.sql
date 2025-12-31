with trips as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
)

select
    pickup_hour,
    
    count(*) as trip_count,
    avg(total_amount) as avg_revenue,
    sum(total_amount) as total_revenue,
    avg(avg_speed_mph) as avg_speed_mph,
    avg(duration_minutes) as avg_duration_minutes

from trips
group by 1
order by pickup_hour