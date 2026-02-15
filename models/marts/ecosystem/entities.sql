{{ config(
    materialized='table',
    schema=var('marts_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    entities 
    ===========================
    All entities .
    
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with entities as (
    select *
    from {{ ref('int_entities') }}
),


-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        entity_id,
        entity_name,
        entity_type,
        entity_sub_type,
        primary_sector_name,
        primary_sector_short_name,
        is_active,
        entity_country,
        is_israeli,
        entity_logo_url,
        entity_finder_url,
        entity_tagline as short_description,
        disclosure_level

    from entities as entities
)

select * from final