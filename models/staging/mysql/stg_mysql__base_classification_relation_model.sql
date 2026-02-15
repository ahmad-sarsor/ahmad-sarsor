{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ source('mysql', 'BaseClassificationRelationModel') }}
)

select
    id,
    type as classification_type,
    parent_key as parent_company_id,
    child_key as child_company_id
from source
