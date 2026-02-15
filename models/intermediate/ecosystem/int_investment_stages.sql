{{ config(
    materialized='view',
    schema=var('intermediate_schema'),
    tags=['ecosystem']
) }}

{#-
    int_investment_stages
    ============================
    Maps entity investment stages to unified categories for reporting.
    
    Stage Categories:
    - Early Stage: Pre-Funding, Pre-Seed, Seed, A Round
    - Early Growth: B Round, C Round
    - Late Growth: D-G Rounds
    - Mature, Public, Acquired
-#}

-- ============================================================================
-- IMPORTS: Staging References
-- ============================================================================

with investment_stages as (
    select * from {{ ref('stg_mysql__new_company_investment_stages') }}
),

-- ============================================================================
-- TRANSFORMATION: Stage Mapping & Categorization
-- ============================================================================

stage_mapping as (
    select
        investment_stage_id,
        entity_id,
        investment_stage,
        case
            when investment_stage = 'Pre-Funding' then 'Early Stage'
            when investment_stage = 'Pre-Seed' then 'Early Stage'
            when investment_stage = 'Seed' then 'Early Stage'
            when investment_stage = 'A Round' then 'Early Stage'
            when investment_stage = 'B Round' then 'Early Growth'
            when investment_stage = 'C Round' then 'Early Growth'
            when investment_stage = 'D Round' then 'Late Growth'
            when investment_stage = 'E Round' then 'Late Growth'
            when investment_stage = 'F Round' then 'Late Growth'
            when investment_stage = 'G Round' then 'Late Growth'
            when investment_stage = 'Mature' then 'Mature'
            when investment_stage = 'Public' then 'Public'
            when investment_stage = 'Acquired' then 'Acquired'
            else 'Other'
        end as stage_category,
        case
            when investment_stage = 'Pre-Funding' then 1
            when investment_stage = 'Pre-Seed' then 2
            when investment_stage = 'Seed' then 3
            when investment_stage = 'A Round' then 4
            when investment_stage = 'B Round' then 5
            when investment_stage = 'C Round' then 6
            when investment_stage = 'D Round' then 7
            when investment_stage = 'E Round' then 8
            when investment_stage = 'F Round' then 9
            when investment_stage = 'G Round' then 10
            when investment_stage = 'Mature' then 11
            when investment_stage = 'Public' then 12
            when investment_stage = 'Acquired' then 13
            else 99
        end as stage_sort_order
    from investment_stages
    where investment_stage is not null
)

select * from stage_mapping