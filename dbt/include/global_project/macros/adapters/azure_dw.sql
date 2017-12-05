{% macro dist(dist) %}
  {%- if dist is not none -%}
    {%- set dist = dist.strip().lower() -%}
    distribution = {{ dist }}
  {%- else -%}
    distribution = round_robin
  {%- endif -%}
{%- endmacro -%}

{% macro statistics(fields) %}
  {%- if fields is not none -%}
    {% for field in fields %}
      create statistics dbt_stats_{{this.name}}_{{field}} on {{this}} ({{field}});
    {% endfor %}
  {%- endif -%}
{% endmacro %}

{% macro azure_dw__create_view_as(identifier, sql) -%}
  create view {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }} as
  {{ hoist_ctes(sql) }}
{% endmacro %}

{% macro azure_dw__create_table_as(temporary, identifier, sql) -%}

  {%- set _dist = config.get('distribution') -%}
  {%- set _statistics = config.get('statistics') -%}

  create table {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }}
  with (
    {{ dist(_dist) }}
  )
  as {{ hoist_ctes(sql) }};

  {{ statistics(_statistics) }}

  update statistics {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }};

{% endmacro %}
