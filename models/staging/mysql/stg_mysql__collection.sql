{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id as collection_id,
    nullif(`name`, '') as collection_name,
    nullif(`description`, '') as collection_description,
    nullif(`type`, '') as collection_type,
    nullif(url_name, '') as collection_url_name,
    -- nullif(companies_to_categories, '') as companies_to_categories,
    -- nullif(map_image_key, '') as map_image_key,
    nullif(creator_id, '') as creator_id,
    nullif(datetime(created_date), datetime('1970-01-01')) as created_datetime,
    nullif(owner_id, '') as owner_id,
    nullif(datetime(owen_date), datetime('1970-01-01')) as own_datetime,
    nullif(ready_date, datetime('1970-01-01')) as ready_datetime,
    nullif(last_modified_date, datetime('1970-01-01')) as last_modified_datetime,

from {{ ref('ext_mysql__collection') }}