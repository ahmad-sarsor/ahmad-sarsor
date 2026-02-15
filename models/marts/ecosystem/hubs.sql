{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    hubs
    ======================
    Accelerators, programs, communities and co-working spaces.
    
    Hub Types: Accelerator, Corporate Accelerator, 
    Entrepreneurship Program, Community, Co-Working Space
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with entities as (
    select *
    from {{ ref('int_entities') }}
),

investments as (
    select *
    from {{ ref('int_investor_investment') }}
),

-- ============================================================================
-- AGGREGATION: Hub Investment Metrics
-- ============================================================================

hub_investments as (
    select
        investor_id,
        max(round_date) as last_investment,
        count(distinct round_id) as total_investments
    from investments
    group by investor_id
),

-- ============================================================================
-- FILTER: Hubs Only
-- ============================================================================

hubs as (
    select *
    from entities
    where entity_type in ('Accelerator', 'Corporate Accelerator', 'Entrepreneurship Program', 'Community', 'Co-Working Space')
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        hubs.entity_id as hub_id,
        hubs.entity_name as hub_name,
        hubs.entity_type as hub_type,
        hubs.entity_sub_type as hub_sub_type,
        hubs.is_active,
        hubs.entity_country as hub_country,
        hubs.is_israeli,
        hubs.founded_year,
        hubs.founded_month,
        hubs.in_israel_since_year,
        hubs.in_israel_since_month,
        hubs.closed_year,
        hubs.closed_month,
        hubs.is_claimed,
        hubs.claimed_date,
        hubs.representing_of_name,
        hubs.num_members,
        hubs.num_claimed_members,
        hubs.num_followers,
        hubs.register_id,
        hubs.crunchbase_id,
        hub_investments.last_investment,
        hub_investments.total_investments,
        hubs.creator_id,
        hubs.creator_name,  
        hubs.creator_email,
        hubs.created_date,
        hubs.last_modifier_id,
        hubs.last_modified_date,
        hubs.bi_verify_key,
        hubs.bi_verify_name,
        hubs.bi_verify_email,
        hubs.bi_verify_date,
        hubs.entity_finder_url as finder_url,
        hubs.entity_logo_url as logo_url,
        hubs.entity_description as description,
        hubs.entity_tagline as short_description,
        hubs.common_misspellings,
        hubs.entity_website as website,
        hubs.entity_teams_page as teams_page,
        hubs.entity_email_domain as email_domain, 
        hubs.hide_reason,
        hubs.disclosure_level
    from hubs
    left join hub_investments on hubs.entity_id = hub_investments.investor_id
)

select * from final