{% macro strategy__materialized_view__full_refresh(relation, sql) %}
    {{ drop_relation_if_exists(relation) }}
    {{ db_api__create_materialized_view_as(relation, sql) }}
{% endmacro %}
