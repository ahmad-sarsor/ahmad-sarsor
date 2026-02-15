{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    investors
    ===========================
    All investors with enriched investment metrics.
    
    Includes: actual sector/stage (from investments),
    target preferences (from profile), exit counts,
    investment activity metrics
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

exits as (
    {{ get_exit_events() }}
),

-- ============================================================================
-- AGGREGATION: Tags & Preferences
-- ============================================================================

investor_tags as (
    select
        entity_id,
        string_agg(tag_name, ', ' order by tag_name) as industry_preferences
    from {{ ref('int_entity_tags') }}
    group by entity_id
),

investor_target_stages as (
    select
        entity_id,
        string_agg(distinct investment_stage, ', ' order by investment_stage) as target_stage,
        string_agg(distinct stage_category, ', ' order by stage_category) as target_stage_categories
    from {{ ref('int_investment_stages') }}
    group by entity_id
),

-- ============================================================================
-- AGGREGATION: Investment Activity Metrics
-- ============================================================================

investors_investments as (
    select 
        investor_id,
        count(distinct round_id) as num_investments,
        max(round_date) as last_investment_date,
        count(distinct company_id) as num_startups_invested
    from investments
    group by investor_id
),

exited_startups as (
    select 
        investments.investor_id,
        count(distinct investments.company_id) as exited_startups
    from investments
    join exits on exits.acquired_id = investments.company_id
    group by investments.investor_id
),

-- ============================================================================
-- AGGREGATION: Actual Sector & Stage (from investments)
-- ============================================================================

investor_top_sector as (
    select 
        investor_id,
        string_agg(primary_sector_name, ', ' order by primary_sector_name) as actual_sector
    from (
        select 
            investments.investor_id,
            entities.primary_sector_name,
            count(distinct investments.company_id) as company_count,
            rank() over (partition by investments.investor_id order by count(distinct investments.company_id) desc) as rn
        from investments
        inner join entities on investments.company_id = entities.entity_id
        where entities.primary_sector_name is not null
        group by investments.investor_id, entities.primary_sector_name
    )
    where rn = 1
    group by investor_id
),

investor_top_stage as (
    select 
        investor_id,
        string_agg(round_type, ', ' order by round_type) as actual_stage
    from (
        select 
            investor_id,
            round_type,
            count(distinct round_id) as round_count,
            rank() over (partition by investor_id order by count(distinct round_id) desc) as rn
        from investments
        where round_type is not null
        group by investor_id, round_type
    )
    where rn = 1
    group by investor_id
),

-- ============================================================================
-- FILTER: Investors Only
-- ============================================================================

investors as (
    select *
    from entities
    where entity_type = 'Investor'
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        investors.entity_id as investor_id,
        investors.entity_name as investor_name,
        investors.entity_sub_type as investor_type,
        {{ get_sector_short_name('investor_top_sector.actual_sector') }} as actual_sector,
        investor_tags.industry_preferences, 
        investors.is_active,
        investors.entity_country as investor_country,
        investors.is_israeli,
        investors.closed_year,
        investors.closed_month,
        investors.is_claimed,
        investors.claimed_date,
        investors.representing_of_name,
        investors.num_members,
        investors.num_claimed_members,
        investors.num_followers,
        investors.register_id,
        investors.crunchbase_id,
        investor_top_stage.actual_stage,
        investor_target_stages.target_stage_categories as target_investment_stages,
        investors_investments.last_investment_date,
        investors_investments.num_investments,
        investors_investments.num_startups_invested,
        exited_startups.exited_startups,
        investors.angel_user_name,
        investors.angel_user_email,
        investors.creator_id,
        investors.creator_name,
        investors.creator_email,
        investors.created_date,
        investors.last_modifier_id,
        investors.last_modified_date,
        investors.bi_verify_key,
        investors.bi_verify_name,
        investors.bi_verify_email,
        investors.bi_verify_date,
        investors.entity_finder_url as finder_url,
        investors.entity_logo_url as logo_url,
        investors.entity_description as description,
        investors.entity_tagline as short_description,
        investors.common_misspellings,
        investors.entity_website as website,
        investors.entity_teams_page as teams_page,
        investors.entity_email_domain as email_domain,
        investors.hide_reason,
        investors.disclosure_level
    from investors
    left join investors_investments on investors_investments.investor_id = investors.entity_id
    left join exited_startups on exited_startups.investor_id = investors.entity_id
    left join investor_top_sector on investor_top_sector.investor_id = investors.entity_id
    left join investor_top_stage on investor_top_stage.investor_id = investors.entity_id
    left join investor_tags on investor_tags.entity_id = investors.entity_id
    left join investor_target_stages on investor_target_stages.entity_id = investors.entity_id
)

select * from final