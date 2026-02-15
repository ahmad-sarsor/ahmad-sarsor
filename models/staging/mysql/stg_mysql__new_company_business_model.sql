{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ source('mysql', 'New_Company_business_model') }}
)

select
    id ,
    origin_entity_id as company_id,
    business_model
from source
