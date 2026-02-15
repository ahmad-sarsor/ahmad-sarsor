{{ config(
    materialized="view",
    schema=var('intermediate_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    int_private_funding
    ===================
    Private funding rounds for Israeli companies.
    Excludes convertible bonds post-IPO and silent rounds.
    
    Includes: round ranking, size categories, prior round details
-#}

-- ============================================================================
-- IMPORTS: Staging References
-- ============================================================================

with events as (
    select *
    from {{ ref('stg_mysql__base_event') }}  
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

companies as (
    select 
        company_id,
        company_name
    from {{ ref('stg_mysql__new_company') }}
    where is_israeli = 1
        and company_type not like 'Foreign Startup'
),

-- ============================================================================
-- TRANSFORMATION: Public Events & Private Funding Filters
-- ============================================================================

public_events as (
    select 
        entity_id as company_id,
        min(event_date) as first_public_date  
    from events
    where event_type in ('POEvent', 'SpacEvent', 'ReverseMergerEvent')
        and event_date is not null
    group by entity_id
),

private_funding_events as (
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
        case 
            when events.funding_type_funding_type = 'Convertible Debt'
                and public_events.first_public_date is not null
                and events.event_date > public_events.first_public_date
            then 'convertible_bonds' 
            else 'private_funding' 
        end as round_category,
        events.created_date as event_created_date,
        events.creator_key,
        events.updater_key,
        events.updated_date as event_updated_date
    from events
    left join public_events on events.entity_id = public_events.company_id
    where event_type like 'FundingRoundEvent' 
        and (funding_type_funding_type in (
            'A Round', 'SAFE', 'Pre-Seed', 'Undisclosed Round', 'Seed', 'B Round',
            'E Round', 'C Round', 'Convertible Debt', 'D Round', 'G Round', 
            'Equity crowdfunding', 'F Round'
        ) or funding_type_funding_type is null)
        and is_silent_round = 0
),

-- ============================================================================
-- ENRICHMENT: Add User Details
-- ============================================================================

enriched_private_funding as (
    select 
        private_funding_events.*,
        users_creator.email as event_creator_email,
        users_creator.first_name as event_creator_first_name,
        users_creator.last_name as event_creator_last_name,
        users_updater.email as event_updater_email,
        users_updater.first_name as event_updater_first_name,
        users_updater.last_name as event_updater_last_name,
        concat(users_creator.first_name, ' ', users_creator.last_name) as event_creator_name,
        concat(users_updater.first_name, ' ', users_updater.last_name) as event_updater_name
    from private_funding_events
    left join users as users_creator on private_funding_events.creator_key = users_creator.user_id
    left join users as users_updater on private_funding_events.updater_key = users_updater.user_id
    where round_category != 'convertible_bonds'
),

-- ============================================================================
-- METRICS: Calculated Fields
-- ============================================================================

prior_round as (
    select 
        company_id,
        round_id,
        round_date,
        lag(round_date) over (partition by company_id order by round_date) as prior_round_date,
        lag(round_type) over (partition by company_id order by round_date) as prior_round_type,
        lag(amount) over (partition by company_id order by round_date) as prior_round_amount
    from enriched_private_funding
),

round_size_category as (
    select
        round_id,
        case 
            when amount < 100000000 then 'Small Round (<100M)'
            when amount >= 100000000 and amount < 500000000 then 'Medium Round (100M-500M)'
            when amount >= 500000000 and amount < 1000000000 then 'Large Round (500M-1B)'
            when amount >= 1000000000 then 'Mega Round (1B+)'
            else 'Undisclosed'
        end as round_size_category
    from enriched_private_funding
),

round_rank as (
    select 
        company_id,
        round_id,
        row_number() over (
            partition by company_id
            order by coalesce(round_date, event_created_date), round_id
        ) as round_rank
    from enriched_private_funding
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        enriched_private_funding.round_id,
        enriched_private_funding.round_date,
        extract(year from enriched_private_funding.round_date) as round_year,
        enriched_private_funding.disclosure_event_level,
        enriched_private_funding.round_type,
        enriched_private_funding.round_category,
        enriched_private_funding.disclosure_event_type_level,
        enriched_private_funding.is_silent_round,
        enriched_private_funding.amount,
        round_size_category.round_size_category,
        enriched_private_funding.disclosure_amount_level,
        enriched_private_funding.company_id,
        companies.company_name,
        round_rank.round_rank,
        prior_round.prior_round_date, 
        prior_round.prior_round_type, 
        prior_round.prior_round_amount, 
        enriched_private_funding.event_created_date,
        enriched_private_funding.event_updated_date,
        enriched_private_funding.event_creator_email,
        enriched_private_funding.event_creator_first_name,
        enriched_private_funding.event_creator_last_name,
        enriched_private_funding.event_updater_email,
        enriched_private_funding.event_updater_first_name,
        enriched_private_funding.event_updater_last_name,
        enriched_private_funding.event_creator_name,
        enriched_private_funding.event_updater_name,
        enriched_private_funding.source
    from enriched_private_funding
    join companies on enriched_private_funding.company_id = companies.company_id
    left join prior_round on enriched_private_funding.round_id = prior_round.round_id
    left join round_size_category on enriched_private_funding.round_id = round_size_category.round_id
    left join round_rank on enriched_private_funding.round_id = round_rank.round_id
)

select * from final