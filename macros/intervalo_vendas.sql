{% macro intervalo_vendas(column, interval) -%}
{{ column }} >= CAST(CURRENT_DATE - INTERVAL '{{ interval }}' DAY AS TIMESTAMP) and {{ column }} < CAST(CURRENT_DATE AS TIMESTAMP)
{%- endmacro %}