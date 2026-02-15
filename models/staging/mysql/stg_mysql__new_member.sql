{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select *

    from {{ ref('ext_mysql__new_member') }}
)

select * except(id, user, company, created_date),
    id as member_id,
    user as user_id,
    company as company_id,
    created_date

from source