{% macro refresh_materialized_view(relation, config) %}
    {{ adapter.dispatch('refresh_materialized_view',(relation, config)) }}
{% endmacro %}

{% macro default__refresh_materialized_view(relation, config) -%}

    { exceptions.raise_not_implemented(
    'refresh_materialized_view not implemented for adapter '+adapter.type()) }}

{% endmacro %}
