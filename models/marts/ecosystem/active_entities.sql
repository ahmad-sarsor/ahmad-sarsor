{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    active_entities (fact table)
    ============================
    Entity activity status per year (2015 to present).
    
    Enables: time-series analysis, ecosystem growth tracking,
    yearly active/opened/closed counts
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with entities as (
    select
        *
    from {{ ref('int_entities') }}
    where entity_type in ('Company', 'Multinational', 'Investor', 'Accelerator', 'Corporate Accelerator', 'Entrepreneurship Program', 'Community', 'Co-Working Space')
    and disclosure_level in ('all', 'aggregation')
),

-- ============================================================================
-- TRANSFORMATION: Year Spine Generation
-- ============================================================================

years as (
    select year
    from unnest(generate_array(2000, extract(year from current_date()))) as year
),

entities_by_year as (
    select
        entities.entity_id,
        entities.entity_name,
        --entities.entity_type,
        case 
        when entity_type in('Accelerator', 'Corporate Accelerator', 'Entrepreneurship Program', 'Community', 'Co-Working Space') then 'Hub'
        else entities.entity_type
        end as entity_type,
        entities.entity_sub_type,
        entities.primary_sector_name,
        entities.founded_date,
        entities.founded_year,
        entities.founded_month,
        entities.closed_year,
        entities.closed_reason,
        entities.is_active,
        entities.is_israeli,
        entities.closed_date,
        entities.disclosure_level,
        years.year
    from entities
    cross join years
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['entity_id', 'year']) }} as activity_id,
        year,
        entity_id,
        entity_name,
        entity_type,
        entity_sub_type,
        primary_sector_name,
        founded_date,
        founded_year,
        is_israeli,
        case
            when founded_year is null then 0
            when founded_year > year then 0
            when closed_year is not null and closed_year < year then 0
            else 1
        end as was_active,
        case
            when founded_year = year then 1
            else 0
        end as was_opened_in_the_year,
        case
            when closed_year = year then 1
            else 0
        end as was_closed_in_the_year,
        case
            when is_active = 1 then 1
            else 0
        end as is_active_today,
        closed_date,
        closed_reason,
        disclosure_level
    from entities_by_year
    where (founded_year is null or year >= founded_year)
        and (closed_year is null or year <= closed_year)
)

select * from final