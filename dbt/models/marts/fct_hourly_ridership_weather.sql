-- One row per hour × transport_mode with the weather of that hour and calendar flags.
with ridership as (
    select * from {{ ref('int_ridership_hourly') }}
),

weather as (
    select * from {{ ref('stg_weather__hourly') }}
),

holidays as (
    select * from {{ ref('turkish_public_holidays') }}
),

joined as (
    select
        r.observed_at,
        cast(r.observed_at as date)                 as date,
        cast(hour(r.observed_at) as integer)        as hour,
        cast(isodow(r.observed_at) as integer)      as day_of_week,
        isodow(r.observed_at) in (6, 7)             as is_weekend,
        -- Half-day holidays (feast eves, Republic Day Eve) start at 13:00.
        coalesce(
            h.date is not null and (not h.is_half_day or hour(r.observed_at) >= 13),
            false
        )                                           as is_holiday,
        r.transport_mode,
        r.passages,
        r.passengers,
        w.temperature_2m,
        w.precipitation,
        w.snowfall,
        w.wind_speed_10m
    from ridership as r
    left join weather as w
        on r.observed_at = w.observed_at
    left join holidays as h
        on cast(r.observed_at as date) = h.date
)

select
    observed_at,
    date,
    hour,
    day_of_week,
    is_weekend,
    is_holiday,
    transport_mode,
    passages,
    passengers,
    temperature_2m,
    precipitation,
    snowfall,
    wind_speed_10m,
    precipitation >= 0.1 as is_rainy,
    case
        when temperature_2m is null then null
        when temperature_2m < 5 then '<5'
        when temperature_2m < 15 then '5-15'
        when temperature_2m < 25 then '15-25'
        else '>=25'
    end as temp_band
from joined
