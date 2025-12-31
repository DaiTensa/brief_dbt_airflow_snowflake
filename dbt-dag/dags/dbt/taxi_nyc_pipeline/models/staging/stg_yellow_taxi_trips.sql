with raw_data as (
    select * from {{ source('raw_taxi_data', 'YELLOW_TAXI_TRIPS') }}
),

clean_data as (
    select
        -- Identifiers
        {{ dbt_utils.generate_surrogate_key(['VENDORID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME', 'PULOCATIONID', 'DOLOCATIONID', 'TRIP_DISTANCE']) }} as trip_id,
        cast(VENDORID as integer) as vendor_id,
        cast(PULOCATIONID as integer) as pickup_location_id,
        cast(DOLOCATIONID as integer) as dropoff_location_id,
        cast(RATECODEID as integer) as rate_code_id,
        STORE_AND_FWD_FLAG as store_and_fwd_flag,

        -- Timestamps
        cast(TPEP_PICKUP_DATETIME as timestamp) as pickup_datetime,
        cast(TPEP_DROPOFF_DATETIME as timestamp) as dropoff_datetime,

        -- Trip info
        cast(PASSENGER_COUNT as integer) as passenger_count,
        cast(TRIP_DISTANCE as numeric(10,2)) as trip_distance,
        
        -- Payment info
        cast(FARE_AMOUNT as numeric(10,2)) as fare_amount,
        cast(EXTRA as numeric(10,2)) as extra,
        cast(MTA_TAX as numeric(10,2)) as mta_tax,
        cast(TIP_AMOUNT as numeric(10,2)) as tip_amount,
        cast(TOLLS_AMOUNT as numeric(10,2)) as tolls_amount,
        cast(IMPROVEMENT_SURCHARGE as numeric(10,2)) as improvement_surcharge,
        cast(TOTAL_AMOUNT as numeric(10,2)) as total_amount,
        cast(CONGESTION_SURCHARGE as numeric(10,2)) as congestion_surcharge,
        cast(AIRPORT_FEE as numeric(10,2)) as airport_fee,
        coalesce(cast(PAYMENT_TYPE as integer), 0) as payment_type

    from raw_data
    where 1=1
    -- Filter negative amounts
    and FARE_AMOUNT >= 0
    and TOTAL_AMOUNT >= 0
    -- Keep only valid trips
    and TPEP_PICKUP_DATETIME < TPEP_DROPOFF_DATETIME
    -- Filter distances (between 0.1 and 100 miles)
    and TRIP_DISTANCE between 0.1 and 100
    -- Exclude null zones
    and PULOCATIONID is not null
    and DOLOCATIONID is not null
)

select
    *,
    datediff('minute', pickup_datetime, dropoff_datetime) as duration_minutes,
    date_part('hour', pickup_datetime) as pickup_hour,
    date_part('day', pickup_datetime) as pickup_day,
    date_part('month', pickup_datetime) as pickup_month,
    date_part('year', pickup_datetime) as pickup_year,
    dayname(pickup_datetime) as pickup_day_name,
    
    -- Average speed (miles per hour)
    case 
        when datediff('minute', pickup_datetime, dropoff_datetime) > 0 
        then (trip_distance / (datediff('minute', pickup_datetime, dropoff_datetime) / 60.0))
        else 0 
    end as avg_speed_mph,

    -- Tip percentage
    case
        when fare_amount > 0 then (tip_amount / fare_amount) * 100
        else 0
    end as tip_percentage

from clean_data