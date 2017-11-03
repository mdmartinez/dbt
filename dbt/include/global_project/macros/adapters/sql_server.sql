{% macro sql_server__create_table_as(temporary, identifier, sql) -%}
  with __dbt_cta as ( {{ sql }} )
  select *
  into {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }}
  from __dbt_cta
{% endmacro %}
