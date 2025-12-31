with trips as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
)

select
    *,
    
    -- Distance Categories
    case
        when trip_distance <= 1 then 'Courts trajets'
        when trip_distance <= 5 then 'Trajets moyens'
        when trip_distance <= 10 then 'Longs trajets'
        else 'Très longs trajets'
    end as distance_category,
    
    -- Time Period Categories
    case
        when pickup_hour between 6 and 9 then 'Rush Matinal'
        when pickup_hour between 10 and 15 then 'Journée'
        when pickup_hour between 16 and 19 then 'Rush Soir'
        when pickup_hour between 20 and 23 then 'Soirée'
        else 'Nuit'
    end as time_period,
    
    -- Day Type (Weekday vs Weekend)
    case
        when pickup_day_name in ('Sat', 'Sun') then 'Weekend'
        else 'Semaine'
    end as day_type

from trips
