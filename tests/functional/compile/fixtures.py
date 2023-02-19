my_ephemeral_model_sql = """
{{ config(materialized = 'ephemeral') }}
select 1 as fun
"""

another_ephemeral_model_sql = """
{{ config(materialized = 'ephemeral') }}
select * from {{ ref('my_ephemeral_model') }}
"""

my_other_model_sql = """
select * from {{ ref('another_ephemeral_model')}}
union all
select 2 as fun
"""
