-- Average hourly passages by transport_mode × hour × day_type × weather condition, indexed
-- against dry hours with the same mode, hour and day_type.
with hourly as (
    select
        transport_mode,
        hour,
        case
            when is_weekend or is_holiday then 'weekend_or_holiday'
            else 'weekday'
        end as day_type,
        -- Snow takes precedence over rain: snowy hours usually also report precipitation.
        case
            when snowfall > 0 then 'snow'
            when is_rainy then 'rain'
            else 'dry'
        end as condition,
        passages
    from {{ ref('fct_hourly_ridership_weather') }}
    -- Hours without weather cannot be classified.
    where precipitation is not null
      and snowfall is not null
),

grouped as (
    select
        transport_mode,
        hour,
        day_type,
        condition,
        avg(passages)               as avg_passages,
        cast(count(*) as integer)   as n_hours
    from hourly
    group by transport_mode, hour, day_type, condition
)

select
    g.transport_mode,
    g.hour,
    g.day_type,
    g.condition,
    g.avg_passages,
    g.n_hours,
    g.avg_passages / nullif(dry.avg_passages, 0) as index_vs_dry
from grouped as g
left join grouped as dry
    on g.transport_mode = dry.transport_mode
    and g.hour = dry.hour
    and g.day_type = dry.day_type
    and dry.condition = 'dry'
