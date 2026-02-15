{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ source('mysql', 'BaseClassificationModel') }}
)

select
    id as classification_id,
    is_lead,
    depth,
    url_name,
    type as classification_type,
    name as sector_name,
    description,
    root_key

from source
