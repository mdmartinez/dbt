{% macro sql_server__create_table_as(temporary, identifier, sql) -%}
  with intermediate as ( {{ sql }} )
  select *
  into {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }}
  from intermediate
{% endmacro %}
