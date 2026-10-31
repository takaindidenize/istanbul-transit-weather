{#- Fails for every combination of the given columns that appears more than once.
    A dependency-free stand-in for dbt_utils.unique_combination_of_columns. -#}
{% test unique_combination_of_columns(model, combination_of_columns) %}

select
    {{ combination_of_columns | join(', ') }},
    count(*) as n_rows
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1

{% endtest %}
