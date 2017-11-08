
{% materialization incremental, adapter='sql_server' -%}
   {{dbt__incremental_sql_server()}}
{% endmaterialization %}

{% materialization incremental, adapter='azure_dw' -%}
   {{dbt__incremental_sql_server()}}
{% endmaterialization %}

{% macro dbt__incremental_sql_server() %}
  {%- set sql_where = config.require('sql_where') -%}
  {%- set unique_key = config.get('unique_key') -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = '##' + model['name'] + '__dbt_incremental_tmp' -%}

  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- set exists_as_table = (existing_type == 'table') -%}
  {%- set should_truncate = (non_destructive_mode and full_refresh_mode and exists_as_table) -%}
  {%- set should_drop = (not should_truncate and (full_refresh_mode or (existing_type not in (none, 'table')))) -%}
  {%- set force_create = (flags.FULL_REFRESH and not flags.NON_DESTRUCTIVE) -%}

  -- setup
  {% if existing_type is none -%}
    -- noop
  {%- elif should_truncate -%}
    {{ adapter.truncate(schema, identifier) }}
  {%- elif should_drop -%}
    {{ adapter.drop(schema, identifier, existing_type) }}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if force_create or not adapter.already_exists(schema, identifier) -%}
    {%- call statement('main') -%}
      {{ create_table_as(False, identifier, sql) }}
    {%- endcall -%}
  {%- else -%}
    {%- set temp_table_sql %}
      with dbt_incr_sbq as (
        {{ sql }}
      )
      select * from dbt_incr_sbq
      where ({{ sql_where }})
        or ({{ sql_where }}) is null
    {%- endset -%}

    {%- call statement() -%}
      {{ create_table_as(True, tmp_identifier, temp_table_sql) }}
    {%- endcall -%}

     {{ adapter.expand_target_column_types(temp_table=tmp_identifier,
                                           to_schema=schema,
                                           to_table=identifier) }}

     {%- call statement('main') -%}
       {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
       {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

       {% if unique_key is not none -%}

         {{ dbt__incremental_delete(schema, model) }}

       {%- endif %}

       insert into {{adapter.quote_schema_and_table(schema, identifier)}} ({{ dest_cols_csv }})
       (
         select {{ dest_cols_csv }}
         from {{ tmp_identifier }}
       );
     {% endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmacro %}
