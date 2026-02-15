{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    ipo
    ================
    Initial Public Offering events for Israeli companies.
    
    Includes: stock exchange details, ticker, capital raised,
    valuation at IPO
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
        disclosure_level
    from {{ ref('int_entities') }}
    where disclosure_level in ('all', 'aggregation')
),

public_company_funding as (
    select *
    from {{ ref('int_public_company_funding') }}   
),

-- ============================================================================
-- ENTITIES: Companies & Stock Exchanges
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
        disclosure_level as disclosure_company_level
    from entities
    where is_israeli = 1 
        and entity_type like 'Company'
),

stock_exchange as (
    select 
        entity_id as stock_exchange_id,     
        entity_name as stock_exchange_name,
        entity_type as stock_exchange_type,
        entity_country as stock_exchange_country,
        is_israeli as stock_exchange_is_israeli
    from entities
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        public_company_funding.event_id,
        public_company_funding.event_type_final as event_type,
        public_company_funding.event_date,
        extract(year from public_company_funding.event_date) as event_year,
        companies.company_id,
        companies.company_name,
        companies.company_type,
        companies.company_primary_sector_name,
        stock_exchange.stock_exchange_name,
        stock_exchange.stock_exchange_country,
        public_company_funding.ticker,
        public_company_funding.source,
        public_company_funding.is_exit,
        public_company_funding.capital_raised,
        public_company_funding.valuation,
        companies.disclosure_company_level,
        public_company_funding.disclosure_amount_level,
        public_company_funding.disclosure_event_level,
        public_company_funding.disclosure_event_type_level,
        public_company_funding.event_created_date,
        public_company_funding.event_creator_name,
        public_company_funding.event_updated_date,
        public_company_funding.event_updater_name,
        public_company_funding.event_updater_email
    from public_company_funding
    inner join companies on public_company_funding.company_id = companies.company_id
    left join stock_exchange on public_company_funding.stock_exchange_key = stock_exchange.stock_exchange_id
    where is_ipo = 1
)

select * from final
order by event_date desc, event_updated_date desc