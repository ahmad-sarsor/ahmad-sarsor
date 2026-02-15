{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ source('mysql', 'Hide_Profile_Reason') }}
)

select
    id as hide_reason_id,
    profile_type,
    reason as reason_description,
    visible as is_visible,
    default_for_new_entity,
    default_for_external_entity
from source
