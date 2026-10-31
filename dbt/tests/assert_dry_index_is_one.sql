-- Dry rows are their own baseline, so their index_vs_dry must be 1.
select *
from {{ ref('mart_weather_effect') }}
where condition = 'dry'
  and avg_passages > 0
  and abs(index_vs_dry - 1) > 1e-9
