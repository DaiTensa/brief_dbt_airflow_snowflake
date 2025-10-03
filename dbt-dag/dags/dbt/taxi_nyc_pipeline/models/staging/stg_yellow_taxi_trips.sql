select
    *
from
    {{source('raw_taxi_data', 'YELLOW_TAXI_TRIPS')}}