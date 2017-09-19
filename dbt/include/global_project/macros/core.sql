{%
macro statement(name=None, fetch_result=False, auto_begin=True)
  '
  statement(name=None, fetch_result=False, auto_begin=True)

  `statement` executes an arbitrary SQL query against a warehouse,
  and optionally stores the results in memory. Most queries run by
  dbt are run by the statement block.

  You *must* call `statement` using a call block. Example usage:

      {% call statement(name="arbitrary_select") -%}
        select * from {{ref("a_table_in_my_project")}}
      {%- endcall %}

  Arguments:

  - `name`: A unique name for this statement that will be used
            to store the SQL result. If `None`, then nothing will
            be stored. Defaults to `None`.
  - `fetch_result`: Whether or not to fetch the actual rows
                    returned by this statement. Use this with care --
                    storing a very large number of rows will result
                    in slowness and/or massive memory usage. Defaults
                    to `False`.
  - `auto_begin`: Whether or not to automatically begin a transaction
                  if one is not currently open. This is useful for
                  queries that cannot be run inside a transaction, for
                  example `VACUUM` in Redshift. Defaults to `True`.
  - `caller`: A special argument provided by dbt when called using a
              `call` block. This macro *must* be called with a `call`
              block.
  '
-%}
  {%- if execute: -%}
    {%- set sql = render(caller()) -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
      {{ write(sql) }}
    {%- endif -%}

    {%- set _, cursor = adapter.add_query(sql, auto_begin=auto_begin) -%}
    {%- if name is not none -%}
      {%- set result = [] if not fetch_result else adapter.get_result_from_cursor(cursor) -%}
      {{ store_result(name, status=adapter.get_status(cursor), data=result) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
