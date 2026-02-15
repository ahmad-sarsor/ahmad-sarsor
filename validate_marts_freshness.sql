{% macro validate_marts_freshness() %}
{% if execute %}
  {% set query %}
  with table_info as (
    select
      concat('`', project_id, '.', dataset_id, '.', table_id, '`') as id,
      timestamp_millis(creation_time) as created_at,
      timestamp_millis(last_modified_time) as last_modified_at,
      date(timestamp_millis(last_modified_time)) as last_modified_date,
    from `finderv2.Marts.__TABLES__`
    where type = 1 -- table (not view / external table)
  )
  , stale as (
    select *
    from table_info
    where date(last_modified_at) < date_sub(current_date(), interval 1 day)
    -- order by last_modified_at
  )
  select * from stale

  -- select IF(
  --   (SELECT COUNT(*) FROM stale) > 0,
  --   ERROR('Stale marts!'),
  --   'OK'
  -- );
  {% endset %}
  {% set results = run_query(query) %}
  
  {#{% do results.print_table() %}#}
  {% do log(results.print_json(), info=True) %}
{% endif %}
{% endmacro %}

{# dbt run-operation validate_marts_freshness --log-format json  #}