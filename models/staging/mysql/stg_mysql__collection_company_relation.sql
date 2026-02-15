{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id,
    nullif(collection_key, '') as collection_id,
    nullif(company_key, '') as company_id,
    nullif(note, '') as note,
    nullif(group_id, '') as group_id,
    custom_order as collection_order,
    datetime(nullif(added_date, date('1970-01-01'))) as added_datetime,
    nullif(note_last_modified_datetime, datetime('1970-01-01')) as note_modified_datetime,

from {{ ref('ext_mysql__collection_company_relation') }}

where company_key is not null and company_key != ''