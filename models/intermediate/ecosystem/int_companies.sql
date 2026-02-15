{{ config(
    materialized="table",
    schema=var('intermediate_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with companies as (
    select *

    from {{ ref('stg_mysql__new_company') }}
)

, classification as (
    select
        classification_id,
        root_key,
        sector_name as classification_sector,
        url_name as classification_url_name,
        -- classification_type,

    from {{ ref('stg_mysql__base_classification_model') }}
)

select
    companies.*,

    -- companies.company_id,
    -- companies.company_name,
    -- companies.primary_sector_key,
    -- classification.classification_sector,
    -- root_classification.classification_sector,

    classification.classification_sector as company_primary_sector,

    nullif(
        root_classification.classification_sector,
        classification.classification_sector
    ) as company_primary_sector_parent,

from companies

left join classification
    on companies.primary_sector_key = classification.classification_id

left join classification as root_classification
    on classification.root_key = root_classification.classification_id

