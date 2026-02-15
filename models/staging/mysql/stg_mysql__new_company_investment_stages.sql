{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['ecosystem']
) }}

with source as (
    select * from {{ ref('ext_mysql__new_company_investment_stages') }}
)

select
    id as investment_stage_id,
    origin_entity_id as entity_id,
    nullif(investment_stages, '') as investment_stage

from source