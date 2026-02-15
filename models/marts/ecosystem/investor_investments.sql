{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    investor_investments
    =================================
    Each investor participation in a funding round.
    
    Includes: lead status, follow-on, first investment ever,
    investment amounts at round and investor level
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with entities as (
    select 
        *
    from {{ ref('int_entities') }}
    where disclosure_level in ('all', 'aggregation')
),

investor_investment as (
    select *
    from {{ ref('int_investor_investment') }}   
),

private_funding as (
    select *
    from {{ ref('int_private_funding') }}   
),

-- ============================================================================
-- ENTITIES: Companies & Investors Preparation
-- ============================================================================

companies as (
    select 
        entity_id as company_id,     
        entity_name as company_name,
        entity_type as company_type,
        entity_sub_type as company_sub_type,
        entity_country as company_country,
        is_israeli as company_is_israeli,
        primary_sector_name as company_primary_sector_name,    
        disclosure_level as disclosure_company_level,
        entity_finder_url as company_finder_url
    from entities
),

investors as (
    select 
        entity_id as investor_id,
        entity_name as investor_name,
        entity_type as investor_type,
        entity_sub_type as investor_sub_type,
        entity_country as investor_country,
        is_israeli as investor_is_israeli,
        disclosure_level as disclosure_investor_level,
        entity_finder_url as investor_finder_url
    from entities
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        investor_investment.investment_id,
        investor_investment.round_id,
        investor_investment.round_date,
        extract(year from investor_investment.round_date) as round_year,
        investor_investment.round_type,
        case 
            when private_funding.round_id is not null then 'Private Funding'
            else investor_investment.round_type
        end as round_category,
        investor_investment.amount as total_round_amount,
        private_funding.disclosure_event_level,
        investor_investment.company_id,
        companies.company_name,
        companies.disclosure_company_level,
        companies.company_finder_url,
        investor_investment.investor_id,
        investors.investor_name,
        investors.investor_type,
        investors.investor_sub_type,
        investors.investor_country,
        investors.disclosure_investor_level,
        investors.investor_finder_url,
        investor_investment.is_lead,
        investor_investment.investor_investment_amount,
        investor_investment.disclosure_invested_level,
        investor_investment.is_follow_on,
        investor_investment.is_first_investment_ever,
        investor_investment.first_investment_date,
        investor_investment.source,
        investor_investment.event_created_date,
        investor_investment.event_creator_name,
        investor_investment.event_creator_email,
        investor_investment.event_updated_date,
        investor_investment.event_updater_name,
        investor_investment.event_updater_email
    from investor_investment
    inner join companies on companies.company_id = investor_investment.company_id
    inner join investors on investors.investor_id = investor_investment.investor_id
    left join private_funding on private_funding.round_id = investor_investment.round_id
)

select * from final
order by round_date desc, event_updated_date desc