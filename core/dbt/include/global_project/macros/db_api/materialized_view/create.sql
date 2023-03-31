{% macro db_api__create_materialized_view_as(relation, sql) %}
    {{ adapter.dispatch('db_api__create_materialized_view_as', 'dbt')(relation, sql) }}
{% endmacro %}

{% macro default_db_api__create_materialized_view_as(relation, sql) -%}

    {{ exceptions.raise_not_implemented(
    'db_api__create_materialized_view_as macro not implemented for adapter '+adapter.type()) }}

{% endmacro %}
