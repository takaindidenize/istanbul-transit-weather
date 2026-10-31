-- İBB hourly public transport at the source grain, typed and cleaned.
with source as (
    select * from {{ source('raw', 'ibb') }}
)

select
    cast(transition_date as timestamp)
        + to_hours(cast(transition_hour as integer))        as observed_at,
    cast(transition_date as date)                           as transition_date,
    cast(transition_hour as tinyint)                        as transition_hour,
    cast(transport_type_id as tinyint)                      as transport_type_id,
    case cast(transport_type_id as tinyint)
        when 1 then 'road'
        when 2 then 'rail'
        when 3 then 'sea'
    end                                                     as transport_mode,
    nullif(trim(road_type), '')                             as road_type,
    nullif(trim(line), '')                                  as line,
    nullif(trim(line_name), '')                             as line_name,
    nullif(trim(transfer_type), '')                         as transfer_type,
    nullif(trim(product_kind), '')                          as product_kind,
    nullif(trim(transaction_type_desc), '')                 as transaction_type_desc,
    nullif(trim(town), '')                                  as town,
    nullif(trim(station_poi_desc_cd), '')                   as station_poi_desc_cd,
    cast(number_of_passage as integer)                      as passages,
    cast(number_of_passenger as integer)                    as passengers,
    cast(source_month as varchar)                           as source_month,
    cast(loaded_at as timestamp)                            as loaded_at
from source
