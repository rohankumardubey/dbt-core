-- MATERIALIZED_VIEW IMPLMENTATION
{% macro create_materialized_view_as(relation, sql, config) %}
    {{ return(adapter.dispatch('create_materialized_view_as',(relation, sql, config)) }}
{% endmacro %}

{% macro default__create_materialized_view_as(relation, sql, config) -%}

    { exceptions.raise_not_implemented(
    'create_materialized_view_as macro not implemented for adapter '+adapter.type()) }}

{% endmacro %}
