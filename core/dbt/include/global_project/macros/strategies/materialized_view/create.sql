{% macro strategy__materialized_view__create(relation, sql) %}
    {{ db_api__create_materialized_view_as(relation, sql) }}
{% endmacro %}
