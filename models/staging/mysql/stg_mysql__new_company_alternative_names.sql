{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ source('mysql', 'New_Company_alternative_names') }}
)

select
    id as alternative_name_id,      
    origin_entity_id as entity_id,  
    alternative_names as alternative_name
from source
