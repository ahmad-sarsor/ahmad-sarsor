{{ config(
    materialized='view',
    schema=var('intermediate_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

{# 
============================================================================================================
MODEL: int_company_sectors
============================================================================================================

PURPOSE:
    Maps companies to their sector hierarchy (sector → sub_sector → sub_sub_sector).
    Each company can have multiple sector assignments, resulting in one row per unique combination.

BUSINESS CONTEXT:
    - Companies are classified into sectors at different depths (0, 1, 2)
    - depth=0: Top-level sector (e.g., "Business Software", "Climate Tech")
    - depth=1: Sub-sector (e.g., "IT, R&D & Data Solutions", "Energy Transition")
    - depth=2: Sub-sub-sector (e.g., "Software Development Tools", "Hydrogen")
    - Parent-child relationships are stored in BaseClassificationRelationModel, !!but some are missing!!

KEY CHALLENGES SOLVED:
    1. Missing Relations: Some depth=2 classifications lack parent relations in source data.
       Solution: Auto-detect parent when company has exactly one depth=1 in same root_key.

    2. Leaf Detection: Need to find the "deepest" classification for each company.
       Solution: Exclude classifications that have children the company is also assigned to.
    
    
    3. Ambiguous Parents: When depth=2 has no parent and multiple depth=1 options exist.
       Solution: Display depth=2 as sub_sector (not sub_sub_sector) to avoid incorrect mapping.

OUTPUT COLUMNS:
    - company_sector_id: Surrogate key (hash of company_id + sector hierarchy)
    - company_id: Company identifier
    - sector: Top-level sector (depth=0)
    - sub_sector: Second-level sector (depth=1, or depth=2 if no parent found)
    - sub_sub_sector: Third-level sector (depth=2 with known parent)

DEPENDENCIES:
    - stg_mysql__base_classification_company_model: Company-to-classification assignments
    - stg_mysql__base_classification_model: Classification definitions (id, name, depth, root_key)
    - stg_mysql__base_classification_relation_model: Parent-child relationships

MAINTENANCE NOTES:
    - If new sectors are added without proper relations, they will appear as sub_sector (not sub_sub_sector)
    - Run qa_missing_sector_relations query to identify missing relations in source data
    - Auto-detection works only when company has exactly one depth=1 per root_key

Hierarchical Data in SQL:
https://www.sqlservertutorial.net/sql-server-basics/sql-server-recursive-cte/
https://learnsql.com/blog/how-to-query-hierarchical-data/

Tree Structures in Databases:
"SQL Antipatterns" by Bill Karwin (Chapter: Naive Trees)
https://www.slideshare.net/billkarwin/models-for-hierarchical-data

============================================================================================================
#}


{# ==================== STEP 1: BASE DATA ==================== #}

with company_classifications as (
    -- Fetches all sector classifications assigned to each company.
    -- Joins company assignments with classification metadata to get depth and root_key.
    -- This is the foundation for all subsequent calculations.
    select 
        base_classification_company.company_id,
        base_classification_company.classification_id,
        base_classification.sector_name as classification_name,
        base_classification.depth,
        base_classification.root_key
    from {{ ref('stg_mysql__base_classification_company_model') }} as base_classification_company
    inner join {{ ref('stg_mysql__base_classification_model') }} as base_classification
        on base_classification_company.classification_id = base_classification.classification_id
    where base_classification_company.classification_category = 'TechnologyClassificationModel'
        and base_classification.classification_type = 'TechnologyClassificationModel'
),

classifications as (
    -- Reference table of all sector classifications (regardless of company assignments).
    -- Used for looking up names and hierarchy information.
    select 
        classification_id,
        sector_name,
        depth,
        root_key
    from {{ ref('stg_mysql__base_classification_model') }}
    where classification_type = 'TechnologyClassificationModel'
),


{# ==================== STEP 2: RELATIONS (ORIGINAL + AUTO-DETECTED) ==================== #}

original_relations as (
    -- Parent-child relationships from source MySQL data.
    -- Maps which depth=1 is the parent of which depth=2.
    -- Note: Some relations are missing in source data and need to be auto-detected.
    select 
        parent_company_id as parent_key, 
        child_company_id as child_key
    from {{ ref('stg_mysql__base_classification_relation_model') }}
    where classification_type = 'TechnologyClassificationModel'
),

company_depth1_count as (
    -- Counts how many depth=1 classifications each company has per root_key.
    -- If count=1, we can safely auto-detect the parent relationship.
    -- If count>1, we cannot determine which depth=1 is the correct parent.
    select 
        company_id,
        root_key,
        count(*) as depth1_count,
        max(classification_id) as single_parent_key  -- Valid only when depth1_count=1
    from company_classifications
    where depth = 1
    group by company_id, root_key
),

auto_detected_relations as (
    -- Automatically creates missing parent-child relations.
    -- Logic: If a company has exactly ONE depth=1 and a depth=2 in the same root_key,
    -- and no relation exists in source data, we infer that depth=1 is the parent.
    -- This handles ~90% of missing relations automatically.
    select distinct
        company_depth1_count.single_parent_key as parent_key,
        depth2.classification_id as child_key
    from company_classifications as depth2
    inner join company_depth1_count
        on depth2.company_id = company_depth1_count.company_id
        and depth2.root_key = company_depth1_count.root_key
        and company_depth1_count.depth1_count = 1  -- Only auto-detect when exactly one depth=1!
    left join original_relations
        on depth2.classification_id = original_relations.child_key
    where depth2.depth = 2
        and original_relations.child_key is null  -- No existing relation
),

sector_relations as (
    -- Combined relations: original from MySQL + auto-detected.
    -- This unified view is used for both leaf detection and hierarchy building.
    select parent_key, child_key from original_relations
    union distinct
    select parent_key, child_key from auto_detected_relations
),


{# ==================== STEP 3: LEAF DETECTION ==================== #}


classifications_with_children as (
    -- Identifies classifications that have children the company is ALSO assigned to.
    -- These are NOT leaves - they should be excluded from final output.
    -- Example: If company has both "Food Tech" and "Food Trade & Services",
    -- then "Food Tech" has a child and is not a leaf.
    select distinct
        cc.company_id,
        cc.classification_id
    from company_classifications as cc
    inner join sector_relations as sr
        on cc.classification_id = sr.parent_key
    inner join company_classifications as child_cc
        on sr.child_key = child_cc.classification_id
        and cc.company_id = child_cc.company_id
),

depth0_with_deeper as (
    -- Identifies depth=0 (sector) classifications where the company also has deeper assignments.
    -- We don't want to show just "Business Software" if company also has "IT, R&D & Data Solutions".
    -- This ensures we always show the most specific classification.
    select distinct
        cc0.company_id,
        cc0.classification_id
    from company_classifications as cc0
    inner join company_classifications as cc_deeper
        on cc0.company_id = cc_deeper.company_id
        and cc0.root_key = cc_deeper.root_key
        and cc_deeper.depth > cc0.depth
    where cc0.depth = 0
),

company_leaves as (
    -- Finds "leaf" classifications - the deepest level for each company in each sector tree.
    -- A leaf is a classification that:
    --   1. Has no children that the company is also assigned to (via relations)
    --   2. Is not a depth=0 when deeper classifications exist
    -- Uses LEFT JOIN + IS NULL pattern because BigQuery doesn't support correlated NOT EXISTS.
    select company_classifications.*
    from company_classifications
    left join classifications_with_children
        on company_classifications.company_id = classifications_with_children.company_id
        and company_classifications.classification_id = classifications_with_children.classification_id
    left join depth0_with_deeper
        on company_classifications.company_id = depth0_with_deeper.company_id
        and company_classifications.classification_id = depth0_with_deeper.classification_id
    where classifications_with_children.classification_id is null  -- No children assigned
        and depth0_with_deeper.classification_id is null           -- Not a depth=0 with deeper exists
),


{# ==================== STEP 4: HIERARCHY BUILDING ==================== #}

sectors as (
    -- Maps root_key to sector name (depth=0).
    -- Used to populate the 'sector' column for all classifications.
    select 
        root_key, 
        sector_name
    from classifications
    where depth = 0
),

leaf_with_hierarchy as (
    -- Builds the full hierarchy for each leaf classification.
    -- Joins to get: sector (via root_key) and parent (via relations for depth=2).
    select 
        company_leaves.company_id,
        company_leaves.classification_id,
        company_leaves.classification_name,
        company_leaves.depth as leaf_depth,
        company_leaves.root_key,
        sectors.sector_name as sector,
        parent_via_relation.sector_name as parent_of_depth2  -- NULL if no relation found
    from company_leaves
    left join sectors 
        on company_leaves.root_key = sectors.root_key
    left join sector_relations as relation_depth2
        on company_leaves.classification_id = relation_depth2.child_key
        and company_leaves.depth = 2
    left join classifications as parent_via_relation
        on relation_depth2.parent_key = parent_via_relation.classification_id
),


{# ==================== STEP 5: FINAL OUTPUT MAPPING ==================== #}

final as (
    -- Maps leaf classifications to the correct output columns based on depth.
    -- Depth mapping logic:
    --   depth=0: sector only, sub_sector=NULL, sub_sub_sector=NULL
    --   depth=1: sector + sub_sector, sub_sub_sector=NULL
    --   depth=2 with parent: sector + sub_sector (from parent) + sub_sub_sector
    --   depth=2 without parent: sector + sub_sector (classification itself), sub_sub_sector=NULL
    --     (This handles cases where parent relation is missing and cannot be auto-detected)
    select distinct
        leaf_with_hierarchy.company_id,
        leaf_with_hierarchy.sector,
        case 
            when leaf_with_hierarchy.leaf_depth = 0 then null
            when leaf_with_hierarchy.leaf_depth = 1 then leaf_with_hierarchy.classification_name
            when leaf_with_hierarchy.leaf_depth = 2 and leaf_with_hierarchy.parent_of_depth2 is not null 
                then leaf_with_hierarchy.parent_of_depth2
            when leaf_with_hierarchy.leaf_depth = 2 then leaf_with_hierarchy.classification_name
        end as sub_sector,
        case 
            when leaf_with_hierarchy.leaf_depth in (0, 1) then null
            when leaf_with_hierarchy.leaf_depth = 2 and leaf_with_hierarchy.parent_of_depth2 is not null 
                then leaf_with_hierarchy.classification_name
            when leaf_with_hierarchy.leaf_depth = 2 then null
        end as sub_sub_sector
    from leaf_with_hierarchy

)


{# ==================== OUTPUT ==================== #}

select 
    {{ dbt_utils.generate_surrogate_key(['company_id', 'sector', 'sub_sector', 'sub_sub_sector']) }} as company_sector_id,
    company_id,
    sector,
    sub_sector,
    sub_sub_sector
from final