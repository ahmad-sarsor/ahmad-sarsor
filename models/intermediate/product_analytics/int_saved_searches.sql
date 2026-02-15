{{ config(
    materialized='view',
    schema=var('intermediate_schema'),
) }}

select
    user_id,
    search_id as saved_search_id,
    `name` as saved_search_name,
    `description` as saved_search_description,
    url as saved_search_url,
    `type` as saved_search_type,
    notification_interval_days,
    date(created_datetime) as saved_search_created_date,
    
    created_datetime as saved_search_created_datetime,
    nullif(last_modified_datetime, created_datetime) as saved_search_last_modified_datetime,

from {{ ref('stg_mysql__saved_searches') }}