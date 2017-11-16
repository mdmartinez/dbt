{% macro azure_dw__create_view_as(identifier, sql) -%}
  create view {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }} as
  {{ sql }}
{% endmacro %}

{% macro azure_dw__create_table_as(temporary, identifier, sql) -%}
  with intermediate as ( {{ sql }} )
  select *
  into {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }}
  from intermediate
{% endmacro %}
