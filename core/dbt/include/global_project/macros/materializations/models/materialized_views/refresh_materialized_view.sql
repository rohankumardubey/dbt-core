{% macro refresh_materialized_view(relation, config) %}
    {{ return(adapter.dispatch('refresh_materialized_view',(relation, config)) }}
{% endmacro %}

{% macro default__refresh_materialized_view(relation, config) -%}

    refresh materialized view {{relation}}

{% endmacro %}
