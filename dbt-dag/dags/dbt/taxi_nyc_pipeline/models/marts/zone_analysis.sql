with trips as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
)

select
    pickup_location_id,
    
    count(*) as trip_count,
    avg(total_amount) as avg_revenue,
    avg(trip_distance) as avg_distance,
    sum(total_amount) as total_revenue,
    avg(tip_percentage) as avg_tip_percentage

from trips
group by 1
order by trip_count desc