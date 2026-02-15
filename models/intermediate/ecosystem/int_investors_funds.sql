{{ config(
    materialized="view",
    schema=var('intermediate_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    int_investors_funds
    ===================
    Investment firm fund raising events.
    Tracks funds raised by investors (VCs, funds, etc.)
-#}

-- ============================================================================
-- IMPORTS: Staging References
-- ============================================================================

with events as (
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
-- TRANSFORMATION: Fund Events with User Details
-- ============================================================================

investments as (
    select 
        events.id as fund_id,
        events.event_date as fund_date,
        events.entity_id as investor_id,
        events.name as fund_name,
        events.amount_amount as invested_amount,
        events.amount_visibility_visibility_type as disclosure_amount_level,
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
    where event_type like 'InvestmentFirmFundingEvent' 
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        fund_id,
        fund_date,
        investor_id,
        fund_name,
        invested_amount,
        disclosure_amount_level,
        disclosure_event_level,
        source,
        event_created_date,
        event_creator_email,
        event_creator_name,
        event_updated_date,
        event_updater_email,
        event_updater_name
    from investments
)

select * from final