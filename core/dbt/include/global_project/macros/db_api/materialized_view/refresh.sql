{% macro db_api__materialized_view__refresh(relation) %}
    {{ adapter.dispatch('refresh_materialized_view', 'dbt')(relation) }}
{% endmacro %}

{% macro default__db_api_refresh_materialized_view(relation) -%}

    {{ exceptions.raise_not_implemented(
    'refresh_materialized_view not implemented for adapter '+adapter.type()) }}

{% endmacro %}
