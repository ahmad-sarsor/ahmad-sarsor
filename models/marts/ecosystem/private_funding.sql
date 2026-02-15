{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    private_funding
    ============================
    Private funding rounds for Israeli companies.
    
    Includes: round size categories, investor origin breakdown,
    prior round details, round ranking
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with entities as (
    select 
        entity_id,
        entity_name,
        entity_type,
        entity_sub_type,
        entity_country,
        is_israeli,
        primary_sector_name,
        disclosure_level,
        entity_finder_url
    from {{ ref('int_entities') }}
    where disclosure_level in ('all', 'aggregation')
),

private_funding_events as (
    select *
    from {{ ref('int_private_funding') }}   
),

investor_investments as (
    select 
        investment_id,
        round_id,
        investor_id,
        investor_type,
        investor_sub_type,
        investor_country,
        case 
            when investor_country = 'Israel' then 'israeli_investor'  
            else 'global_investor' 
        end as investor_origin
    from {{ ref('int_investor_investment') }}
),

-- ============================================================================
-- AGGREGATION: Round Participation Metrics
-- ============================================================================

round_participation as (
    select 
        round_id,
        count(distinct case when investor_origin = 'israeli_investor' then investor_id end) as num_israeli_investors,
        count(distinct case when investor_origin = 'global_investor' then investor_id end) as num_global_investors,
        count(distinct investor_id) as num_investors,
        case 
            when count(distinct case when investor_origin = 'global_investor' then investor_id end) = 0 
                then 'Israeli Only'
            when count(distinct case when investor_origin = 'israeli_investor' then investor_id end) = 0 
                then 'Global Only'
            else 'Both'
        end as investors_origin,
        count(distinct case when investor_sub_type = 'Angel' then investor_id end) as num_angels
    from investor_investments
    group by round_id
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        private_funding_events.round_id,
        private_funding_events.round_date,
        private_funding_events.round_year,
        private_funding_events.disclosure_event_level,
        private_funding_events.round_type,
        private_funding_events.disclosure_event_type_level,
        private_funding_events.amount,
        private_funding_events.round_size_category,
        private_funding_events.disclosure_amount_level,
        private_funding_events.company_id,
        private_funding_events.company_name,
        entities.primary_sector_name as company_primary_sector_name,
        entities.disclosure_level as disclosure_company_level,
        private_funding_events.round_rank,
        entities.entity_finder_url as company_finder_url,
        round_participation.investors_origin,
        round_participation.num_investors,
        round_participation.num_angels,
        round_participation.num_israeli_investors,
        round_participation.num_global_investors,
        private_funding_events.source,
        private_funding_events.prior_round_date,
        private_funding_events.prior_round_type,
        private_funding_events.prior_round_amount,
        private_funding_events.event_created_date,
        private_funding_events.event_creator_name,
        private_funding_events.event_creator_email,
        private_funding_events.event_updated_date,
        private_funding_events.event_updater_name,
        private_funding_events.event_updater_email
    from private_funding_events
    inner join entities on private_funding_events.company_id = entities.entity_id
    left join round_participation on private_funding_events.round_id = round_participation.round_id
)

select * from final
order by round_date desc, event_updated_date desc