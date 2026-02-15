{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    multinationals
    ================================
    Multinational corporations with presence in Israel.
    
    Includes: acquisition metrics, Israel presence details,
    employee counts, corporate arms
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
-- AGGREGATION: Acquisition Metrics
-- ============================================================================

mnc_acquisitions as (
    select
        acquirer_id,
        max(event_date) as last_acquisition_date,
        count(distinct event_id) as total_acquisitions,
        sum(amount) as total_acquisition_amount
    from acquisitions
    group by acquirer_id
),

-- ============================================================================
-- FILTER: Multinationals Only
-- ============================================================================

mncs as (
    select *
    from entities
    where entity_type = 'Multinational'
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        mncs.entity_id as mnc_id,
        mncs.entity_name as mnc_name,
        mncs.entity_country as mnc_country,
        mncs.is_active,
        mncs.founded_year,
        mncs.founded_month,
        mncs.closed_year as closing_in_israel_year,
        mncs.closed_month as closing_in_israel_month,
        mncs.is_claimed,
        mncs.claimed_date,
        mncs.num_members,
        mncs.num_claimed_members,
        mncs.num_followers,
        mncs.crunchbase_id,
        mnc_acquisitions.last_acquisition_date,
        mnc_acquisitions.total_acquisitions,
        mnc_acquisitions.total_acquisition_amount,
        mncs.israeli_employees_exact,
        mncs.israeli_employees_range,   
        mncs.global_employees_exact,
        mncs.global_employees_range,
        mncs.in_israel_since_year,
        mncs.in_israel_since_month,
        mncs.market_cap,
        mncs.market_cap_update,
        mncs.arms,
        mncs.investor_arm_name,
        mncs.creator_id,
        mncs.creator_name,
        mncs.creator_email,
        mncs.created_date,
        mncs.last_modifier_id,
        mncs.last_modified_date,
        mncs.bi_verify_key,
        mncs.bi_verify_name,
        mncs.bi_verify_email,
        mncs.bi_verify_date,
        mncs.entity_finder_url as finder_url,
        mncs.entity_logo_url as logo_url,
        mncs.entity_logo as logo,
        mncs.entity_description as description,
        mncs.entity_tagline as short_description,
        mncs.common_misspellings,
        mncs.entity_website as website,
        mncs.entity_email_domain as email_domain,
        mncs.hide_reason,
        mncs.disclosure_level
    from mncs
    left join mnc_acquisitions on mncs.entity_id = mnc_acquisitions.acquirer_id
)

select * from final