-- Ridership aggregated to hour × transport_mode. Rows with an unknown transport_type_id are
-- dropped here (they warn in staging).
select
    observed_at,
    transport_mode,
    cast(sum(passages) as bigint)   as passages,
    cast(sum(passengers) as bigint) as passengers
from {{ ref('stg_ibb__hourly_transport') }}
where transport_mode is not null
group by observed_at, transport_mode
