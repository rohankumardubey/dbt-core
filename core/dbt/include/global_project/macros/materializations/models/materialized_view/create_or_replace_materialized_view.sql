{% macro create_or_replace_materialized_view() %}
 {{ adapter.dispatch('create_or_replace_materialized_view') }}

{% endmacro %}

{% macro default__create_or_replace_materialized_view() -%}

    { exceptions.raise_not_implemented(
    'create_or_replace_materialized_view macro not implemented for adapter '+adapter.type()) }}

{% endmacro %}
