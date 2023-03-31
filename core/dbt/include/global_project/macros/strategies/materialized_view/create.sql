{% macro strategy__materialized_view__create(relation, sql) %}
    {{ db_api__materialized_view__create(relation, sql) }}
{% endmacro %}
