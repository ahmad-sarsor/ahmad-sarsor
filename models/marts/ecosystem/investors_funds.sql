{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    investors_funds
    ============================
    Fund raising events by investment firms.
    
    Includes: fund ranking per investor, fund amounts,
    investor details
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with entities as (
    select *
    from {{ ref('int_entities') }}
    where disclosure_level in ('all', 'aggregation')
),

investors_funds as (
    select *
    from {{ ref('int_investors_funds') }}   
),

-- ============================================================================
-- ENTITIES: Investors Preparation
-- ============================================================================

investors as (
    select 
        entity_id as investor_id,
        entity_name as investor_name,
        entity_country as investor_country,
        is_israeli as investor_is_israeli,
        entity_type as investor_type,
        entity_sub_type as investor_sub_type,
        entity_finder_url as investor_finder_url,
        entity_logo_url as investor_logo_url,
        disclosure_level as disclosure_investor_level,
        hide_reason as investor_hide_reason,
        is_active as investor_is_active
    from entities 
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        investors_funds.fund_id,
        investors_funds.fund_date,
        extract(year from investors_funds.fund_date) as fund_year,
        investors_funds.investor_id,
        investors.investor_name,
        investors.investor_type,
        investors.investor_sub_type,
        investors.investor_country,
        investors.investor_is_israeli,
        investors_funds.fund_name,
        row_number() over (partition by investors_funds.investor_id order by investors_funds.fund_date) as fund_rank,
        investors_funds.invested_amount,
        investors_funds.disclosure_amount_level,
        investors_funds.disclosure_event_level,
        investors_funds.source,
        investors_funds.event_created_date,
        investors_funds.event_creator_email,
        investors_funds.event_creator_name,
        investors_funds.event_updated_date,
        investors_funds.event_updater_email,
        investors_funds.event_updater_name
    from investors_funds
    inner join investors on investors_funds.investor_id = investors.investor_id
)

select * from final
order by fund_date desc, event_updated_date desc