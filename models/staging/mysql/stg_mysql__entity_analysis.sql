{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id,
    nullif(name, '') as name,
    nullif(email_domain, '') as email_domain,
    nullif(type, '') as type,
    nullif(description, '') as description,
    nullif(sector, '') as sector,
    created_date,

from {{ ref('ext_mysql__entity_analysis') }}

where nullif(type, '') is not null