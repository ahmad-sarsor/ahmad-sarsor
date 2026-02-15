{{ config(
    materialized='table',
    schema=var('dwh_dev'),
    tags=['ecosystem']
) }}

with company_sectors as (
    select *
    from {{ ref('int_core_technology') }}
),

entities as (
    select *
    from {{ ref('int_entities') }}
),

companies as (
    select 
        entity_id as company_id,     
        entity_name as company_name,
        primary_sector_name
    from entities
    where entity_type like 'Company'
)
select 
    company_sectors.company_sector_id,
    company_sectors.company_id,
    companies.company_name,
    --companies.primary_sector_name,
    company_sectors.sector,
    company_sectors.sub_sector,
    company_sectors.sub_sub_sector
from company_sectors 
    inner join companies on company_sectors.company_id = companies.company_id
order by company_id,sector,sub_sector,sub_sub_sector