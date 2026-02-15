{{ config(
    materialized='table',
    schema=var('dwh_dev'),
    tags=['ecosystem']
) }}

-- ============================================================
-- OTHER EVENTS MART
-- Dynamic approach: includes all events from base_event that 
-- don't exist in other event tables. New event types and fields
-- will be automatically included.
-- ============================================================

-- ============================================================
-- 1. IMPORTS
-- ============================================================
with base_events as (
    select *
    from {{ ref('stg_mysql__base_event') }}
    where event_type != 'ClosingEvent'
),

entities as (
    select 
        entity_id,
        entity_name,
        entity_type,
        entity_sub_type,
        entity_country,
        is_israeli,
        primary_sector_name,
        disclosure_level,
        entity_finder_url,
        entity_logo_url,
        is_active
    from {{ ref('int_entities') }}
),

users as (
    select 
        user_id,
        email,
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name
    from {{ ref('stg_mysql__new_user') }}
),

-- ============================================================
-- 2. COLLECT EVENT IDS FROM OTHER TABLES
-- ============================================================
private_funding_ids as (
    select round_id as event_id from {{ ref('int_private_funding') }}
),

acquisition_ids as (
    select event_id from {{ ref('int_acquisitions') }}
),

public_funding_ids as (
    select event_id from {{ ref('int_public_company_funding') }}
),

investor_fund_ids as (
    select fund_id as event_id from {{ ref('int_investors_funds') }}
),

all_existing_ids as (
    select event_id from private_funding_ids
    union all
    select event_id from acquisition_ids
    union all
    select event_id from public_funding_ids
    union all
    select event_id from investor_fund_ids
),

-- ============================================================
-- 3. FILTER: Events not in other tables (all fields from base_events)
-- ============================================================
other_events as (
    select 
        id as event_id,
        * except(id)
    from base_events
    where id not in (select event_id from all_existing_ids)
),

-- ============================================================
-- 4. ADD RELATED ENTITY DETAILS
-- ============================================================
institution_entities as (
    select 
        entity_id as institution_id,
        entity_name as institution_name,
        entity_type as institution_type,
        entity_finder_url as institution_finder_url
    from entities
),

target_entities as (
    select 
        entity_id as target_id,
        entity_name as target_name,
        entity_type as target_type,
        entity_country as target_country,
        entity_finder_url as target_finder_url
    from entities
),

-- ============================================================
-- 5. FINAL: Assembly with Entity Details
-- ============================================================
final as (
    select 
        -- All base event fields
        other_events.*,
        
        -- Calculated fields
        case 
            when other_events.event_type = 'ClosingEvent' then 'Company Closure'
            when other_events.event_type = 'GrantEvent' then 'Grant'
            when other_events.event_type = 'GraduationEvent' then 'Accelerator Graduation'
            when other_events.event_type = 'CommunityInvolvementEvent' then 'Community Involvement'
            when other_events.event_type = 'DelistingEvent' then 'Stock Delisting'
            when other_events.event_type = 'ICOEvent' then 'ICO/Token Sale'
            when other_events.event_type = 'MNAEvent' then 'Acquisition Made'
            else other_events.event_type
        end as event_category,
        extract(year from other_events.event_date) as event_year,
        
        -- Primary entity details
        entities.entity_name,
        entities.entity_type,
        entities.entity_sub_type,
        entities.entity_country,
        entities.primary_sector_name,
        entities.disclosure_level as entity_disclosure_level,
        entities.entity_finder_url,
        entities.is_active as entity_is_active,
        
        -- Institution entity details (for GraduationEvent)
        institution_entities.institution_name,
        institution_entities.institution_type,
        institution_entities.institution_finder_url,
        
        -- Target entity details (for MNAEvent acquirer side)
        target_entities.target_name,
        target_entities.target_type,
        target_entities.target_country,
        target_entities.target_finder_url,
        
        -- User details
        users_creator.full_name as event_creator_name,
        users_creator.email as event_creator_email,
        users_updater.full_name as event_updater_name,
        users_updater.email as event_updater_email

    from other_events
    inner join entities on other_events.entity_id = entities.entity_id
    left join institution_entities on other_events.institution_entity_key = institution_entities.institution_id
    left join target_entities on other_events.other_party_entity_key = target_entities.target_id
    left join users as users_creator on other_events.creator_key = users_creator.user_id
    left join users as users_updater on other_events.updater_key = users_updater.user_id
)

select *
from final
order by event_date desc, updated_date desc