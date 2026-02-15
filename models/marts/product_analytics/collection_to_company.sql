{{ config(
    materialized='table',
    schema=var('marts_schema'),
    unique_key='record_id',
    tags=['product_analytics']
) }}

with map as (
    select
        id as record_id,
        collection_id as list_id,
        group_id,
        company_id,
        added_datetime as company_added_datetime,
        collection_order as company_list_order,
        note,
        note_modified_datetime as note_created_or_modified_datetime,

    from {{ ref('stg_mysql__collection_company_relation') }}
)
-- select count(*) from map /*

, collections as (
    select
        collection_id as list_id,
        created_datetime as list_created_datetime,
        last_modified_datetime as list_last_modified_datetime,

    from {{ ref('stg_mysql__collection') }}
)
-- select count(*) from collections /*

, groups_cte as (
    select
        group_id,
        collection_id as list_id,
        group_name,
        group_entity_type,
        group_description,
        group_modified_datetime as group_created_or_modified_datetime,

    from {{ ref('stg_mysql__collection_groups') }}
)
-- select count(*) from groups_cte /*

, companies as (
    select
        {{ company_dimensions() }}

    from {{ ref('int_companies') }}
)

, merged as (
    select
        map.record_id,
        map.list_id,
        collections.list_created_datetime,
        collections.list_last_modified_datetime,
        map.group_id,
        groups_cte.* except (list_id, group_id),
        map.company_id,
        map.company_list_order,
        map.company_added_datetime,
        companies.* except (company_id),
        map.note,
        map.note_created_or_modified_datetime,

    from map
    left join collections using (list_id)
    left join groups_cte using (list_id, group_id)
    left join companies using (company_id)
)
select * from merged
-- select count(*) from merged /*

/**/