{{ config(
    materialized="view",
    schema=var('intermediate_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    int_investor_investment
    =======================
    Links investors to funding rounds with enriched details.
    Identifies first-time investments and lead investors.
-#}

-- ============================================================================
-- IMPORTS: Staging References
-- ============================================================================

with investors_events as (
    select *
    from {{ ref('stg_mysql__base_event_investors') }}  
),

events as (
    select *
    from {{ ref('stg_mysql__base_event') }}  
),

investors as (
    select 
        company_id as investor_id,
        company_name as investor_name,
        company_type as investor_type,
        company_subtype as investor_sub_type,
        country as investor_country
    from {{ ref('stg_mysql__new_company') }}  
),

users as (
    select 
        user_id,
        email,
        first_name,
        last_name,
        first_name || ' ' || last_name as user_full_name
    from {{ ref('stg_mysql__new_user') }}
),

-- ============================================================================
-- TRANSFORMATION: Round & Investor Details
-- ============================================================================

investors_investments as (
    select 
        investment_id,
        round_id,
        is_lead,
        investor_id,
        disclosure_investor_investment_level,
        disclosure_amount_level,
        investor_amount,
        is_follow_on,
        investors_first_investment
    from investors_events
),

investments as (
    select 
        events.id as round_id,
        events.entity_id as company_id,
        events.event_date as round_date,
        events.is_silent_round,
        events.funding_type_funding_type as round_type,
        events.amount_amount as amount,
        events.amount_visibility_visibility_type as disclosure_amount_level,
        events.funding_type_visibility_visibility_type as disclosure_event_type_level,
        events.visibility_visibility_type as disclosure_event_level,
        events.source,
        events.created_date as event_created_date,
        users_creator.email as event_creator_email,
        users_creator.first_name || ' ' || users_creator.last_name as event_creator_name,
        events.updated_date as event_updated_date,
        users_updater.email as event_updater_email,
        users_updater.first_name || ' ' || users_updater.last_name as event_updater_name
    from events
    left join users as users_creator on events.creator_key = users_creator.user_id
    left join users as users_updater on events.updater_key = users_updater.user_id
    where event_type like 'FundingRoundEvent' 
        and is_silent_round = 0
),

-- ============================================================================
-- AGGREGATION: First Investment Calculation
-- ============================================================================

first_investments as (
    select 
        investor_id,
        min(round_date) as first_investment_date
    from investors_investments
    inner join investments on investments.round_id = investors_investments.round_id
    where round_date is not null
    group by investor_id
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        investors_investments.investment_id,
        investors_investments.round_id,
        investors_investments.investor_id,
        investors.investor_country,
        investors.investor_type,
        investors.investor_sub_type,
        investors_investments.is_lead,
        investors_investments.investor_amount as investor_investment_amount,
        investors_investments.disclosure_amount_level as disclosure_invested_level,
        investors_investments.is_follow_on,
        investments.round_date,
        case when first_investments.first_investment_date = investments.round_date then 1 else 0 end as is_first_investment_ever,
        first_investments.first_investment_date,
        investments.round_type,
        investments.amount,
        investments.company_id,
        investments.is_silent_round,
        investments.source,
        investments.event_created_date,
        investments.event_creator_name,
        investments.event_creator_email,
        investments.event_updated_date,
        investments.event_updater_name,
        investments.event_updater_email
    from investments
    inner join investors_investments on investments.round_id = investors_investments.round_id
    inner join investors on investors.investor_id = investors_investments.investor_id
    left join first_investments on first_investments.investor_id = investors_investments.investor_id
)

select * from final