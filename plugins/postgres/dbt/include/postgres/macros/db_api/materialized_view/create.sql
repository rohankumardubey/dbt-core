{% macro postgres__db_api__materialized_view__create(relation, sql) %}
    {% set proxy_view = postgres__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}
