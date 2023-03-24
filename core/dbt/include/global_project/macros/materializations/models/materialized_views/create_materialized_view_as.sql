-- MATERIALIZED_VIEW IMPLMENTATION
{% macro create_materialized_view_as(relation, sql, config) %}
    {{ return(adapter.dispatch('create_materialized_view_as',(relation, sql, config)) }}
{% endmacro %}

{% macro default__create_materialized_view_as(relation, sql, config) -%}

    create materialized view {{relation}} as (
        {{sql}}
    )

{% endmacro %}
