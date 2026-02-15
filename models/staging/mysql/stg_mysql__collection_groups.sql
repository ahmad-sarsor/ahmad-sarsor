{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id as group_id,
    nullif(owner_id, '') as owner_id,
    nullif(watchlist_id, '') as collection_id,
    nullif(entity_type, '') as group_entity_type,
    nullif(group_name, '') as group_name,
    nullif(group_description, '') as group_description,
    nullif(last_update, datetime('1970-01-01')) as group_modified_datetime,

from {{ ref('ext_mysql__collection_groups') }}