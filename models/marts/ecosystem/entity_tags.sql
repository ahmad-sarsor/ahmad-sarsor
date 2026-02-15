{{ config(
    materialized='table',
    schema=var('marts_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

{#-
    entity_tags
    ==========================
    Links entities to their assigned tags.
    Enables filtering and analysis by custom tags.
-#}

-- ============================================================================
-- IMPORTS: Intermediate References
-- ============================================================================

with tags as (
    select
        tag_id,
        entity_id,
        tag_name
    from {{ ref('int_entity_tags') }}
),

entities as (
    select
        entity_id,
        entity_name,
        entity_type
    from {{ ref('int_entities') }}
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select
        tags.tag_id,
        tags.entity_id,
        entities.entity_name,
        entities.entity_type,
        tags.tag_name
    from tags
    inner join entities on tags.entity_id = entities.entity_id
)

select * from final