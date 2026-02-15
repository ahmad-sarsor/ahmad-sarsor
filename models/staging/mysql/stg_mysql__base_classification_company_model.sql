{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ source('mysql', 'BaseClassificationCompanyModel') }}
)

select
    id,
    company_type,
    classification_category,
    classification_key as classification_id,
    company_key as company_id
from source
