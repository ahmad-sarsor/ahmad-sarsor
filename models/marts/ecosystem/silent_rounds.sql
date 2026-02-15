{{ config(
    materialized='table',
    schema=var('dwh_dev'),
    tags=['ecosystem']
) }}

-- ============================================================
-- IMPORTS
-- ============================================================
with entities as (
    select entity_id,
           entity_name,
           entity_type,
           entity_sub_type,
           entity_country,
           is_israeli,
           primary_sector_name,
           disclosure_level,
           entity_finder_url,
           is_minimal_profile,
           hide_reason,
           hide_reason_id

    from {{ ref('int_entities') }}
),

silent_rounds as (
    select *
    from {{ ref('int_silent_rounds') }}   
),

-- ============================================================
-- 2. ENTITY PREPARATION
-- ============================================================
companies as (
    select 
        entity_id as company_id,     
        entity_name as company_name,
        entity_type as company_type,
        entity_sub_type as company_sub_type,
        entity_country as company_country ,
        is_israeli as company_is_israeli,
        primary_sector_name as company_primary_sector_name,    
        disclosure_level as disclosure_company_level,
        entity_finder_url as company_finder_url,
        hide_reason as company_hide_reason,
        hide_reason_id as company_hide_reason_id
    from entities
),

investors as (
    select 
        entity_id as investor_id,
        entity_name as investor_name,
        entity_type as investor_type,
        entity_sub_type as investor_sub_type,
        entity_country as investor_country ,
        is_israeli as investor_is_israeli,
        disclosure_level as disclosure_investor_level,
        entity_finder_url as investor_finder_url,
        hide_reason as investor_hide_reason,
        hide_reason_id as investor_hide_reason_id
    from entities
),


-- ============================================================
-- 3. FINAL: Assembly
-- ============================================================
final as (
    select 
        silent_rounds.investment_id,
        silent_rounds.round_id,
        silent_rounds.investor_id,
        silent_rounds.round_type,
        investors.investor_name,
        investors.investor_type,
        investors.investor_sub_type,
        investors.investor_country,
        investors.investor_is_israeli,
        investors.investor_hide_reason,
        investors.investor_finder_url,
        silent_rounds.company_id,
        companies.company_name,
        companies.company_type,
        companies.company_sub_type,
        companies.company_primary_sector_name,
        companies.company_hide_reason,
        companies.company_finder_url,
        silent_rounds.source,
        silent_rounds.event_created_date,
        silent_rounds.event_creator_email,
        silent_rounds.event_creator_name,
        silent_rounds.event_updated_date,
        silent_rounds.event_updater_email,
        silent_rounds.event_updater_name

    from silent_rounds as silent_rounds 
        inner join investors as investors on investors.investor_id=silent_rounds.investor_id
        inner join companies as companies on companies.company_id=silent_rounds.company_id
)


select *
From final
--order by round_date desc, event_updated_date desc 