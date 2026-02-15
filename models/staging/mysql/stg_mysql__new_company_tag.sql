{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ source('mysql', 'New_Company_Tag') }}
)

select
    id as tag_id,
    NULLIF(TRIM(old_key), '') as old_tag_id,
    NULLIF(TRIM(tag_name), '') as tag_name,
    NULLIF(TRIM(tag), '') as tag,
    company as company_id,
    NULLIF(TRIM(company_type), '') as company_type,
    NULLIF(TRIM(report_tag), '') as report_tag
from source
