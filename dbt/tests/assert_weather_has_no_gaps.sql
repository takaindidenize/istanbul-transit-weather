-- Every hour between the first and last ridership date (00:00 of the first day to 23:00 of the
-- last day) must have a weather row. Returns the missing hours.
with bounds as (
    select
        cast(min(observed_at) as date)                                as first_day,
        cast(max(observed_at) as date)                                as last_day
    from {{ ref('int_ridership_hourly') }}
),

expected_hours as (
    select unnest(generate_series(
        cast(first_day as timestamp),
        cast(last_day as timestamp) + interval 23 hour,
        interval 1 hour
    )) as observed_at
    from bounds
    where first_day is not null
)

select e.observed_at
from expected_hours as e
left join {{ ref('stg_weather__hourly') }} as w
    on e.observed_at = w.observed_at
where w.observed_at is null
