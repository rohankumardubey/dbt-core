{% macro strategy__materialized_view__full__refresh(relation, sql, backup_relation=None) %}
    {{ adapter.dispatch('strategy__materialized_view__full__refresh', 'dbt')(relation, sql, backup_relation) }}
{% endmacro %}


{% macro default__strategy__materialized_view__full__refresh(relation, sql, backup_relation=None) %}
    {% if backup_relation %}
        {{ adapter.rename_relation(target_relation, backup_relation) }}
    {% endif %}
    {{ drop_relation_if_exists(relation) }}
    {{ db_api__materialized_view__create(relation, sql) }}
{% endmacro %}
