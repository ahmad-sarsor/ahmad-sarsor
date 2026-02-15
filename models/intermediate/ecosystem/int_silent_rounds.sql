{{ config(
    materialized="view",
    schema=var('intermediate_eco_schema'),
    tags=['ecosystem']
) }}

/*
============================================================
SILENT ROUNDS WITH INVESTORS
============================================================
Shows all investors participating in silent (undisclosed) funding rounds.
Mirrors the legacy query from MySQL for silent round investor analysis.

Business Context:
- Track which investors participate in confidential rounds
- Analyze investor behavior in undisclosed deals
- Support data quality checks for silent round coverage

Note: This model filters for silent rounds (is_silent_round = 1)
============================================================
*/

-- ============================================================
-- 1. IMPORTS: Staging References
-- ============================================================
with events as (
    select *
    from {{ ref('stg_mysql__base_event') }}  
),

investors_events as (
    select *
    from {{ ref('stg_mysql__base_event_investors') }}  
),

-- Using int_companies for enriched company data with sectors

users as (
    select 
        user_id,
        email,
        first_name,
        last_name
    from {{ ref('stg_mysql__new_user') }}
),

-- Member positions - using external query since staging doesn't include position
-- TODO: Add position field to stg_mysql__new_member
member_positions as (
    select
        user as user_id,
        string_agg(position, ', ') as aggregated_positions
    from {{ ref('ext_mysql__new_member') }}
    where position is not null and position != ''
    group by user
),


-- ============================================================
-- 3. ENRICHMENT: Join Investors & Companies
-- ============================================================
silent_round_investors as (
select  investment_id,
        round_id,
        is_lead,
        investor_id,
        disclosure_investor_investment_level,
        disclosure_amount_level,
        investor_amount,
        is_follow_on,
        investors_first_investment,

    from investors_events as investors_events 
),
silent_rounds as (
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
        users_updater.first_name || ' ' || users_updater.last_name as event_updater_name,

    from events as events 
        left join users as users_creator on events.creator_key = users_creator.user_id
        left join users as users_updater on events.updater_key = users_updater.user_id
    where events.is_silent_round =1 
        ),
-- ============================================================
-- 4. FINAL: Deduplicate and Output
-- ============================================================
final as (
    select 
        silent_round_investors.investment_id,
        silent_rounds.round_id,
        silent_round_investors.investor_id,
        silent_rounds.round_date,
        silent_rounds.company_id,
        silent_rounds.round_type,
        silent_rounds.source,
        silent_rounds.event_created_date,
        silent_rounds.event_creator_email,
        silent_rounds.event_creator_name,
        silent_rounds.event_updated_date,
        silent_rounds.event_updater_email,
        silent_rounds.event_updater_name

        
    from silent_round_investors as silent_round_investors 
         inner join silent_rounds as silent_rounds on silent_rounds.round_id=silent_round_investors.round_id
)

select * from final
--where round_id like 'nHpnBFTgTnRvobDVSIueLwGzMyaY5zdCDBsTHkZ6G7vZ9FuDab42k6'