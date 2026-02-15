{% macro _get_table_schema(table) %}
{% if execute %}
  {% set query %}
  select column_name, data_type
  from {{ '.'.join(ref(table).name.rsplit('.', 1)[0]) }}.INFORMATION_SCHEMA.COLUMNS
  {% endset %}

  {% set results = run_query(query) %}
  {% do log(results.print_json(), info=True) %}
{% endif %}
{% endmacro %}

/*{#
dbt run-operation _get_table_schema --target prod --args "{table: search_filters}" --log-format json
#}*/