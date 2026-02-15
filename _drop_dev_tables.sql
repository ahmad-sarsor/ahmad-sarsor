{% macro _drop_dev_tables(mode='drop') %}
{% if execute and target.name == 'dev' %}
  {% set query %}
  select string_agg(concat(
    "{{ mode }} ", if(table_type like '%TABLE%', 'TABLE', table_type), " ",table_schema,".",   table_name, ";" 
  ), '\n') as query
  from {{ target.schema }}.INFORMATION_SCHEMA.TABLES

  {% if mode == 'truncate' %}
    where lower(table_type) like '%table%'
  {% endif %}
  {% endset %}

  {% set results = run_query(query) %}
  {% set stmt = results[0].query %}
  {% do log(stmt, info=True) %}

  {% set results = run_query(stmt) %}
  {% do log(results.print_json(), info=True) %}
{% endif %}
{% endmacro %}

/*{#
dbt run-operation _drop_dev_tables --log-format json
dbt run-operation _drop_dev_tables --args "{mode: truncate}"
#}*/