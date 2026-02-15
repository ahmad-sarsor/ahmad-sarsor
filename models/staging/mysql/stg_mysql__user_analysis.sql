{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id,
    nullif(user_id, '') as user_id,
    nullif(entity_analysis_id, '') as entity_analysis_id,
    nullif(title_analysis_id, '') as title_analysis_id,
    date,

from {{ ref('ext_mysql__user_analysis') }}