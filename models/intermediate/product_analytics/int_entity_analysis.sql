{{ config(
    materialized='view',
    schema=var('intermediate_schema'),
    tags=['product_analytics']
) }}

select
    user_id,
    name as entity_analysis_name,
    sector as entity_analysis_sector,
    type as entity_analysis_type,

from {{ ref('stg_mysql__entity_analysis') }} ea

join {{ ref('stg_mysql__user_analysis') }} ua
    on ua.entity_analysis_id = ea.id

qualify row_number() over (
    partition by user_id
    order by created_date desc
) = 1