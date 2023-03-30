{% macro postgres__create_materialized_view_as(relation, sql) %}
    {% set proxy_view = postgres__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}
