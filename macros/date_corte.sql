{% macro date_corte(months=60) -%}
CAST(CURRENT_DATE - INTERVAL '{{ months }}' MONTH AS TIMESTAMP)
{%- endmacro %}