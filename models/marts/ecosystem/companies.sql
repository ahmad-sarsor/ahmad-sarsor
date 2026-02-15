{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    companies 
    ===========================
    All companies with enriched metrics.
    
    Includes: funding totals, acquisition history, sectors,
    exit status, employee counts
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

private_funding as (
    select *
    from {{ ref('int_private_funding') }}
),

sectors as (
    select *
    from {{ ref('int_sector_classifications') }}
),

public_funding as (
    select *
    from {{ ref('int_public_company_funding') }}
),
exits as (
    {{ get_exit_events() }}
),

-- ============================================================================
-- AGGREGATION: Sector Grouping
-- ============================================================================

company_sectors as (
    select 
        company_id,
        string_agg(concat('"', sector, '"'), ', ' order by sector) as sectors,
        string_agg(concat('"', sub_sector, '"'), ', ' order by sub_sector) as sub_sectors,
        string_agg(concat('"', sub_sub_sector, '"'), ', ' order by sub_sub_sector) as sub_sub_sectors
    from (
        select distinct
            company_id,
            sector,
            sub_sector,
            sub_sub_sector
        from sectors
    )
    group by company_id
),

-- ============================================================================
-- AGGREGATION: Funding Metrics
-- ============================================================================

private_funding_agg as (
    select 
        company_id,
        sum(amount) as private_funding_amount,
        count(distinct round_id) as total_private_rounds,
        max(round_date) as last_private_funding_date
    from private_funding
    group by company_id
),

public_funding_agg as (
    select 
        company_id,
        sum(capital_raised) as public_funding_amount,
        count(distinct event_id) as total_public_rounds,
        max(event_date) as last_public_funding_date
    from public_funding
    where event_type_final in ('PIPE','Public Offering')
    group by company_id
),

-- ============================================================================
-- AGGREGATION: Acquisition Metrics
-- ============================================================================

acquired_metrics as (
    select 
        acquired_id,
        count(event_id) as was_acquired_count
    from acquisitions
    group by acquired_id
),

acquirer_metrics as (
    select 
        acquirer_id,
        count(event_id) as acquired_other_count
    from acquisitions
    group by acquirer_id
),

last_acquisition as (
    select 
        acquired_id,
        amount as last_acquired_amount,
        event_date as last_acquisition_date,
        acquirer_id as last_acquirer_id,
        acquirers.entity_name as last_acquirer_name,
        acquirers.entity_country as last_acquirer_country
    from acquisitions
    inner join entities as acquirers on acquisitions.acquirer_id = acquirers.entity_id
    qualify row_number() over (partition by acquired_id order by event_date desc) = 1
),

-- ============================================================================
-- FILTER: Companies Only
-- ============================================================================

companies as (
    select *
    from entities
    where entity_type in ('Foreign Startup', 'Company')
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        companies.entity_id as company_id,
        companies.entity_name as company_name,
        companies.entity_type as company_type,
        companies.entity_sub_type as company_sub_type,
        companies.primary_sector_name,
        companies.business_model,
        companies.geographic_markets,
        companies.primary_sector_short_name,
        company_sectors.sectors,
        company_sectors.sub_sectors,
        company_sectors.sub_sub_sectors,
        companies.is_active,
        companies.entity_country as company_country,
        companies.is_israeli,
        companies.founded_year,
        companies.founded_month,
        companies.closed_date,
        companies.closed_year,
        companies.closed_month,
        companies.is_claimed,
        companies.claimed_date,
        companies.num_members, 
        companies.num_claimed_members,
        companies.num_followers,
        companies.market_cap,
        companies.market_cap_update as market_cap_date,
        companies.register_id,
        companies.crunchbase_id,
        companies.israeli_employees_exact,
        companies.israeli_employees_range,
        companies.global_employees_exact,
        companies.global_employees_range,
        companies.product_stage,
        companies.internal_funding_stage as funding_stage,
        private_funding_agg.private_funding_amount,
        private_funding_agg.total_private_rounds,
        coalesce(private_funding_agg.private_funding_amount, 0) + coalesce(public_funding_agg.public_funding_amount, 0) as total_funding_amount,
        coalesce(private_funding_agg.total_private_rounds, 0) + coalesce(public_funding_agg.total_public_rounds, 0) as total_rounds,
        private_funding_agg.last_private_funding_date,
        if(companies.entity_id in (select acquired_id from acquisitions), 1, 0) as is_acquired,
        if(companies.entity_id in (select acquired_id from exits where is_exit = 1), 1, 0) as is_exited,
        exits.event_date as exited_date,
        last_acquisition.last_acquisition_date,
        last_acquisition.last_acquirer_id,
        last_acquisition.last_acquirer_name,
        last_acquisition.last_acquired_amount,
        last_acquisition.last_acquirer_country,
        acquired_metrics.was_acquired_count,
        acquirer_metrics.acquired_other_count,
        companies.creator_id,
        companies.creator_name,
        companies.creator_email,
        companies.created_date,
        companies.last_modifier_id,
        companies.last_modified_date,
        companies.bi_verify_key,
        companies.bi_verify_name,
        companies.bi_verify_email,
        companies.bi_verify_date,
        companies.entity_finder_url as finder_url,
        companies.entity_logo_url as logo_url,
        companies.entity_description as description,
        companies.entity_tagline as short_description,
        companies.common_misspellings,
        companies.entity_website as website,
        companies.entity_careers_page as careers_page,
        companies.entity_email_domain as email_domain,
        companies.hide_reason,
        companies.disclosure_level
    from companies
    left join company_sectors on companies.entity_id = company_sectors.company_id
    left join last_acquisition on companies.entity_id = last_acquisition.acquired_id
    left join exits on companies.entity_id = exits.acquired_id and exits.is_exit = 1 and exits.exit_order = 1
    left join acquired_metrics on companies.entity_id = acquired_metrics.acquired_id
    left join acquirer_metrics on companies.entity_id = acquirer_metrics.acquirer_id
    left join private_funding_agg on companies.entity_id = private_funding_agg.company_id
    left join public_funding_agg on companies.entity_id = public_funding_agg.company_id
)

select * from final