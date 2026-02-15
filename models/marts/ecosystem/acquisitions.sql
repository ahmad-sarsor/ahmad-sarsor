{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    acquisitions
    =========================
    M&A transactions with acquired and acquirer details.
    
    Includes: deal size categories, acquirer experience tiers,
    acquirer categories (Financial/Strategic/Israeli)
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with entities as (
    select *
    from {{ ref('int_entities') }}
    
),

acquisitions as (
    select *
    from {{ ref('int_acquisitions') }}   
),

-- ============================================================================
-- ENTITIES: Acquired & Acquirer Preparation
-- ============================================================================

acquireds as (
    select 
        entity_id as acquired_id,
        entity_name as acquired_name,
        entity_country as acquired_country,
        entity_type as acquired_type,
        entity_sub_type as acquired_subtype,
        primary_sector_name as acquired_primary_sector,
        entity_finder_url as finder_url,
        entity_logo_url as logo_url,
        disclosure_level,
        hide_reason,
        is_active
    from entities
    where entity_type like 'Company'
    and disclosure_level in ('all', 'aggregation')
),

acquirers as (
    select 
        entity_id as acquirer_id,
        entity_name as acquirer_name,
        entity_country as acquirer_country,
        is_israeli,
        entity_type as acquirer_type,
        entity_sub_type as acquirer_sub_type,
        entity_finder_url as finder_url,
        entity_logo_url as logo_url,
        primary_sector_name as acquirer_primary_sector,
        disclosure_level,
        hide_reason,
        is_active
    from entities 
),

-- ============================================================================
-- METRICS: Deal Size & Acquirer Analysis
-- ============================================================================

deal_size as (
    select
        event_id,
        case
            when amount < 100000000 then 'Small (<100M)'
            when amount >= 100000000 and amount < 1000000000 then 'Medium (100M-1B)'
            when amount >= 1000000000 then 'Large (1B+)'
            else 'Undisclosed'
        end as deal_size_category,
        amount
    from acquisitions
),

acquirer_category as (
    select 
        acquirer_id,
        case 
            when acquirer_type = 'Investor' then 'Financial Acquirer'
            when acquirer_type in ('Multinational', 'Foreign Startup') then 'Strategic Acquirer'
            when acquirer_type = 'Company' and is_israeli = 1 then 'Israeli Company'
            else 'Other'
        end as acquirer_category
    from acquirers
),

first_acquirer_acquisition as (
    select 
        acquirer_id,
        min(event_date) as first_acquirer_acquisition_date
    from acquisitions
    group by acquirer_id
),

acquirer_deal_number as (
    select
        acquirer_id,
        event_id,
        row_number() over (
            partition by acquirer_id
            order by coalesce(event_date, event_created_date), event_id
        ) as acquirer_deal_number
    from acquisitions
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        acquisitions.event_id,
        acquisitions.mna_type,
        acquisitions.event_date,
        extract(year from acquisitions.event_date) as event_year,
        acquisitions.amount,
        deal_size.deal_size_category,
        acquisitions.acquired_id,
        acquireds.acquired_name,
        acquireds.is_active as acquired_is_active,
        acquireds.acquired_primary_sector,
        acquireds.finder_url as acquired_finder_url,
        acquireds.logo_url as acquired_logo_url,
        acquireds.disclosure_level as disclosure_acquired_level,
        acquireds.hide_reason as acquired_hide_reason,
        acquisitions.is_exit,
        acquisitions.is_first_mna,
        acquisitions.acquirer_id,
        acquirers.acquirer_name,
        acquirers.acquirer_type,
        acquirers.acquirer_sub_type,
        acquirers.is_active as acquirer_is_active,
        acquirers.acquirer_country,
        acquirers.is_israeli as acquirer_is_israeli,
        first_acquirer_acquisition.first_acquirer_acquisition_date,
        case 
            when acquirer_deal_number.acquirer_deal_number = 1 then 'First Acquisition'
            when acquirer_deal_number.acquirer_deal_number between 2 and 5 then '2-5 Acquisition'
            else '6+ Acquisition'   
        end as acquirer_experience_tier,
        acquirer_category.acquirer_category,
        acquirers.finder_url as acquirer_finder_url,
        acquirers.logo_url as acquirer_logo_url,
        acquirers.disclosure_level as disclosure_acquirer_level,
        acquirers.hide_reason as acquirer_hide_reason,
        acquisitions.source,
        acquisitions.event_created_date,
        acquisitions.event_creator_name,
        acquisitions.event_creator_email,
        acquisitions.event_updated_date,
        acquisitions.event_updater_name,
        acquisitions.event_updater_email
    from acquisitions
    join acquireds on acquisitions.acquired_id = acquireds.acquired_id
    join acquirers on acquisitions.acquirer_id = acquirers.acquirer_id
    join deal_size on acquisitions.event_id = deal_size.event_id
    join acquirer_category on acquisitions.acquirer_id = acquirer_category.acquirer_id
    join first_acquirer_acquisition on acquisitions.acquirer_id = first_acquirer_acquisition.acquirer_id
    join acquirer_deal_number on acquisitions.event_id = acquirer_deal_number.event_id
)

select * from final
order by event_date desc, event_updated_date desc