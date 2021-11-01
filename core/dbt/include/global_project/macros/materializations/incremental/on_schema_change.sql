{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
   
   {% if on_schema_change not in ['sync_all_columns', 'append_new_columns', 'fail', 'ignore'] %}
     
     {% set log_message = 'Invalid value for on_schema_change (%s) specified. Setting default value of %s.' % (on_schema_change, default) %}
     {% do log(log_message) %}
     
     {{ return(default) }}

   {% else %}

     {{ return(on_schema_change) }}
   
   {% endif %}

{% endmacro %}

{% macro compare_columns(source_columns, target_columns, should_include) %}

  {% set result = [] %}
  {% set source_names = source_columns | map(attribute = 'column') | list %}
  {% set target_names = target_columns | map(attribute = 'column') | list %}
   
   {# --check whether the name attribute exists in the target - this does not perform a data type check #}
   {% for sc in source_columns %}
     {% if (sc.name in target_names) == should_include %}
        {{ result.append(sc) }}
     {% endif %}
   {% endfor %}
  
  {{ return(result) }}

{% endmacro %}

{% macro diff_columns(source_columns, target_columns) %}
  {{ return(compare_columns(source_columns, target_columns, false) ) }}
{% endmacro %}

{% macro intersect_columns(source_columns, target_columns) %}
  {{ return(compare_columns(source_columns, target_columns, true) ) }}
{% endmacro %}

{% macro diff_column_data_types(source_columns, target_columns) %}
  
  {% set result = [] %}
  {% for sc in source_columns %}
    {% set tc = target_columns | selectattr("name", "equalto", sc.name) | list | first %}
    {% if tc %}
      {% if sc.data_type != tc.data_type %}
        {{ result.append( { 'column_name': tc.name, 'new_type': sc.data_type } ) }} 
      {% endif %}
    {% endif %}
  {% endfor %}

  {{ return(result) }}

{% endmacro %}


{% macro check_for_schema_changes(source_relation, target_relation) %}
  
  {% set schema_changed = False %}
  
  {%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}
  {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set source_not_in_target = diff_columns(source_columns, target_columns) -%}
  {%- set target_not_in_source = diff_columns(target_columns, source_columns) -%}
  {%- set in_target_and_source = intersect_columns(target_columns, source_columns) -%}

  {% set new_target_types = diff_column_data_types(source_columns, target_columns) %}

  {% if source_not_in_target != [] %}
    {% set schema_changed = True %}
  {% elif target_not_in_source != [] or new_target_types != [] %}
    {% set schema_changed = True %}
  {% elif new_target_types != [] %}
    {% set schema_changed = True %}
  {% endif %}
  
  {% set changes_dict = {
    'schema_changed': schema_changed,
    'source_not_in_target': source_not_in_target,
    'target_not_in_source': target_not_in_source,
    'in_target_and_source': in_target_and_source,
    'target_columns': target_columns,
    'new_target_types': new_target_types
  } %}

  {% set msg %}
    In {{ target_relation }}:
        Schema changed: {{ schema_changed }}
        Source columns not in target: {{ source_not_in_target }}
        Target columns not in source: {{ target_not_in_source }}
        New column types: {{ new_target_types }}
  {% endset %}
  
  {% do log(msg) %}

  {{ return(changes_dict) }}

{% endmacro %}


{% macro sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}
  
  {%- set add_to_target_arr = schema_changes_dict['source_not_in_target'] -%}

  {%- if on_schema_change == 'append_new_columns'-%}
     {%- if add_to_target_arr | length > 0 -%}
       {%- do alter_relation_add_remove_columns(target_relation, add_to_target_arr, none) -%}
     {%- endif -%}
  
  {% elif on_schema_change == 'sync_all_columns' %}
     {%- set remove_from_target_arr = schema_changes_dict['target_not_in_source'] -%}
     {%- set new_target_types = schema_changes_dict['new_target_types'] -%}
  
     {% if add_to_target_arr | length > 0 or remove_from_target_arr | length > 0 %} 
       {%- do alter_relation_add_remove_columns(target_relation, add_to_target_arr, remove_from_target_arr) -%}
     {% endif %}

     {% if new_target_types != [] %}
       {% for ntt in new_target_types %}
         {% set column_name = ntt['column_name'] %}
         {% set new_type = ntt['new_type'] %}
         {% do alter_column_type(target_relation, column_name, new_type) %}
       {% endfor %}
     {% endif %}
  
  {% endif %}

  {% set schema_change_message %}
    In {{ target_relation }}:
        Schema change approach: {{ on_schema_change }}
        Columns added: {{ add_to_target_arr }}
        Columns removed: {{ remove_from_target_arr }}
        Data types changed: {{ new_target_types }}
  {% endset %}
  
  {% do log(schema_change_message) %}
  
{% endmacro %}


{% macro process_schema_changes(on_schema_change, source_relation, target_relation) %}
    
    {% if on_schema_change == 'ignore' %}

     {{ return({}) }}

    {% else %}
    
      {% set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) %}
      
      {% if schema_changes_dict['schema_changed'] %}
    
        {% if on_schema_change == 'fail' %}
        
          {% set fail_msg %}
              The source and target schemas on this incremental model are out of sync!
              They can be reconciled in several ways: 
                - set the `on_schema_change` config to either append_new_columns or sync_all_columns, depending on your situation.
                - Re-run the incremental model with `full_refresh: True` to update the target schema.
                - update the schema manually and re-run the process.
          {% endset %}
          
          {% do exceptions.raise_compiler_error(fail_msg) %}
        
        {# -- unless we ignore, run the sync operation per the config #}
        {% else %}
          
          {% do sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}
        
        {% endif %}
      
      {% endif %}

      {{ return(schema_changes_dict) }}
    
    {% endif %}

{% endmacro %}