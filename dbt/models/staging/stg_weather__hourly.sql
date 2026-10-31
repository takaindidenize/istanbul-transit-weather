-- Open-Meteo hourly weather for Istanbul (Europe/Istanbul local time, no DST since 2016).
with source as (
    select * from {{ source('raw', 'weather') }}
)

select
    cast(observed_at as timestamp)              as observed_at,
    cast(observed_at as date)                   as observed_date,
    cast(hour(observed_at) as integer)          as observed_hour,
    cast(temperature_2m as double)              as temperature_2m,
    cast(precipitation as double)               as precipitation,
    cast(rain as double)                        as rain,
    cast(snowfall as double)                    as snowfall,
    cast(wind_speed_10m as double)              as wind_speed_10m,
    cast(relative_humidity_2m as double)        as relative_humidity_2m,
    cast(weather_code as smallint)              as weather_code,
    cast(loaded_at as timestamp)                as loaded_at
from source
