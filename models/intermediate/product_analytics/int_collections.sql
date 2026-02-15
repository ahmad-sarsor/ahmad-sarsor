{{ config(
    materialized='view',
    schema=var('intermediate_schema'),
    tags=['product_analytics']
) }}

select
    collection_id as list_id,
    -- coalesce(owner_id, creator_id) as user_id,
    owner_id,
    creator_id,
    collection_name as list_name,
    collection_description as list_description,
    collection_type as list_type,
    date(created_datetime) as list_created_date,
    
    created_datetime as list_created_datetime,
    -- nullif(own_datetime, created_datetime) as own_datetime,
    -- nullif(ready_datetime, created_datetime) as ready_datetime,
    nullif(last_modified_datetime, created_datetime) as list_last_modified_datetime,

from {{ ref('stg_mysql__collection') }}
