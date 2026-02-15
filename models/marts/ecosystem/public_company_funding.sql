{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    public_company_funding
    ===================================
    All public company funding events.
    
    Event Types: IPO, SPAC, Reverse Merger, Public Offering,
    PIPE, Convertible Bond
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with entities as (
    select *
    from {{ ref('int_entities') }}
    where disclosure_level in ('all', 'aggregation')
),

events as (
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
-- TRANSFORMATION: Public Funding Events
-- ============================================================================

public_fundings as (
    select 
        event_id,
        event_type,
        event_type_final,
        event_date,
        company_id,
        is_exit,
        is_ipo,
        capital_raised,
        stock_exchange_key,
        ticker,
        valuation,
        disclosure_amount_level,
        disclosure_event_type_level,
        disclosure_event_level,
        source,
        event_created_date,
        event_creator_name,
        event_creator_email,
        event_updated_date,
        event_updater_name,
        event_updater_email
    from events
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        public_fundings.event_id,
        public_fundings.event_type_final as event_type,
        public_fundings.event_date,
        extract(year from public_fundings.event_date) as event_year,
        companies.company_id,
        companies.company_name,
        companies.company_type,
        companies.company_primary_sector_name,
        public_fundings.is_ipo,
        public_fundings.is_exit,
        public_fundings.capital_raised,
        stock_exchange.stock_exchange_name,
        stock_exchange.stock_exchange_country,
        public_fundings.ticker,
        public_fundings.valuation,
        public_fundings.source,
        companies.disclosure_company_level,
        public_fundings.disclosure_event_level,
        public_fundings.disclosure_event_type_level,      
        public_fundings.event_created_date,
        public_fundings.event_creator_email,
        public_fundings.event_creator_name,
        public_fundings.event_updated_date,
        public_fundings.event_updater_email,
        public_fundings.event_updater_name
    from public_fundings
    inner join companies on public_fundings.company_id = companies.company_id
    left join stock_exchange on public_fundings.stock_exchange_key = stock_exchange.stock_exchange_id
)

select * from final