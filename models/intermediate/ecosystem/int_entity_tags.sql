{{ config(
    materialized='view',
    schema=var('intermediate_eco_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with tags as (
    select
        tag_id,
        company_id as entity_id,
        tag_name,
    from {{ ref('stg_mysql__new_company_tag') }}
    where tag_name is not null
)

select * from tags