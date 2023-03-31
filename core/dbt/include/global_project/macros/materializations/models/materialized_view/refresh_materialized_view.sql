{% macro refresh_materialized_view(relation) %}
    {{ adapter.dispatch('refresh_materialized_view', 'dbt')(relation) }}
{% endmacro %}

{% macro default__refresh_materialized_view(relation) -%}

    {{ exceptions.raise_not_implemented(
    'refresh_materialized_view not implemented for adapter '+adapter.type()) }}

{% endmacro %}
