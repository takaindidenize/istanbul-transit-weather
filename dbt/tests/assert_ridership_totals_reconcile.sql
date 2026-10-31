-- Aggregation must not lose or duplicate passages: totals per mode in the hourly and daily marts
-- must equal the staging totals. Returns the modes that do not reconcile.
with staging as (
    select transport_mode, sum(passages) as passages
    from {{ ref('stg_ibb__hourly_transport') }}
    where transport_mode is not null
    group by transport_mode
),

hourly as (
    select transport_mode, sum(passages) as passages
    from {{ ref('fct_hourly_ridership_weather') }}
    group by transport_mode
),

daily as (
    select transport_mode, sum(passages) as passages
    from {{ ref('agg_daily_ridership_weather') }}
    group by transport_mode
)

select
    coalesce(s.transport_mode, h.transport_mode, d.transport_mode) as transport_mode,
    s.passages as staging_passages,
    h.passages as hourly_passages,
    d.passages as daily_passages
from staging as s
full outer join hourly as h on s.transport_mode = h.transport_mode
full outer join daily as d on coalesce(s.transport_mode, h.transport_mode) = d.transport_mode
where s.passages is distinct from h.passages
   or s.passages is distinct from d.passages
