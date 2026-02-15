{{ config(
    materialized='table',
    schema=var('history_schema'),
    pre_hook="{{ json_macros(schema=var('history_schema')) }}",
    unique_key='record_id',
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

{%- set table = 'New_Company' -%}

{#
{% set columns = dbt_utils.star(ref('stg_mysql__new_modification')) %}
-- select "{{ columns }}" /*
#}

{%- set columns = [
    'name',
    'tag_line',
    'description',
    'primary_sector_key',
    'homepage',
    'stage',
    'claimed',
    'claimed_date',
    'employees_exact',
    'product_stage',
    'headquarter_address',
    'market_capital',
    'minimal_profile',
    'is_climatetech_relevant',
    'employees_overall_exact',
    'hide_reason'
] -%}

with source as (
  select
    id as modification_id,
    * except (id),

  from  {{ ref('stg_mysql__new_modification') }}

  -- where entity = 'gtfiCvMQMi2Gq9yYbfJ59OlHwQHrBraK263BmEaDknu8ch8hSZkLU8'
)
-- select * from source limit 1000 /*

, inserts as (
  select
    modification_id,
    modified_table,
    entity,
    date(timestamp) as date,
    timestamp,
    action,
    key,
    value

  from source

  cross join unnest({{ var('history_schema') or target.schema }}.json_extract_keys(added_values)) as key with offset
  inner join unnest({{ var('history_schema') or target.schema }}.json_extract_values(added_values)) as value with offset
  using (offset)

  where action = 'INSERT'
  and modified_table = '{{ table }}'
)
-- select * from inserts order by timestamp desc /*

, updates as (
  select
    modification_id,
    modified_table,
    entity,
    date(timestamp) as date,
    timestamp,
    action,
    property_name as key,
    property_value as value

  from source

  where action = 'UPDATE'
  and modified_table = '{{ table }}'
)
-- select * from updates order by timestamp desc /*

, unioned as (
  select * from inserts
  union all
  select * from updates
)
-- select * from unioned /*

, pivoted as (
    select *

    from unioned

    pivot(
        any_value(value)
        for key
        in {{ array_to_sql_list(columns) }}
    )
)
-- select * from pivoted order by modified_table, entity, timestamp /*

, forward_fill as (
    select
        modification_id,
        modified_table,
        entity,
        date,
        timestamp,
        action,

        {% for column in columns %}
        coalesce(
            {{ column }},
            last_value({{ column }} ignore nulls) over (
                partition by modified_table, entity
                order by timestamp
                range between unbounded preceding and current row
            )
        ) as {{ column }},
        {% endfor %}

    from pivoted
)
-- select * from forward_fill order by modified_table, entity, timestamp /*

, daily as (
    select
        {{ dbt_utils.generate_surrogate_key([
          'modified_table',
          'entity',
          'date',
        ]) }} as record_id,
        
        -- modified_table,
        entity,
        date,

        {% for column in columns %}
        max_by({{ column }}, timestamp) as {{ column }},
        {% endfor %}

    from forward_fill

    group by all
)
select * from daily


-- select * from daily order by modified_table, entity, date /*

-- select modified_table, entity, count(distinct action) actions, count(distinct date(timestamp)) dates
-- from `finder-353810.Mysql.New_Modification`
-- where modified_table = 'New_Company'
-- and action in ('UPDATE', 'INSERT')
-- and date_trunc(date(timestamp), month) = '2024-12-01'
-- group by all
-- having actions > 1 and dates > 1
-- order by actions desc, dates desc
-- limit 10

/**/