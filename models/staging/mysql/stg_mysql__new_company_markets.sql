{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']

) }}

with source as (
    select * from {{ source('mysql', 'New_Company_markets') }}
)

select
    id as market_id,
    origin_entity_id as company_id,
    markets as market
from source