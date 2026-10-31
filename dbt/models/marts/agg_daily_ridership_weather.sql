-- One row per date × transport_mode. Daily weather is computed over all hours of the day from
-- the weather source, not only over the hours in which a mode carried passengers.
with ridership as (
    select
        date,
        day_of_week,
        is_weekend,
        transport_mode,
        cast(sum(passages) as bigint)   as passages,
        cast(sum(passengers) as bigint) as passengers
    from {{ ref('fct_hourly_ridership_weather') }}
    group by date, day_of_week, is_weekend, transport_mode
),

daily_weather as (
    select
        observed_date                                               as date,
        avg(temperature_2m)                                         as avg_temperature,
        sum(precipitation)                                          as total_precipitation,
        cast(count(*) filter (where precipitation >= 0.1) as integer) as rainy_hours
    from {{ ref('stg_weather__hourly') }}
    group by observed_date
),

holidays as (
    select * from {{ ref('turkish_public_holidays') }}
)

select
    r.date,
    r.day_of_week,
    r.is_weekend,
    -- Only full-day holidays count at daily grain; half days are mostly regular mornings.
    coalesce(h.date is not null and not h.is_half_day, false) as is_holiday,
    r.transport_mode,
    r.passages,
    r.passengers,
    w.avg_temperature,
    w.total_precipitation,
    w.rainy_hours
from ridership as r
left join daily_weather as w
    on r.date = w.date
left join holidays as h
    on r.date = h.date
