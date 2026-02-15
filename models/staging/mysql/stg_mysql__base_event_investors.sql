{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select 
    id as investment_id,
    investors_lead_partner_key,
    origin_entity_id as round_id,
    coalesce(investors_is_lead,0) as is_lead,
    coalesce(investors_entity_visibility_visibility_type,'Public') as disclosure_investor_investment_level,
    coalesce(investors_amount_visibility_visibility_type,'Public') as disclosure_amount_level,
    nullif(investors_amount_amount,0) as investor_amount,
    ifnull(investors_is_follow_on,0) as is_follow_on,
    investors_first_investment,
    investors_entity_entity_key as investor_id

from {{ ref('ext_mysql__base_event_investors') }}
