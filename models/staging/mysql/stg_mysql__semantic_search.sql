{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    * except (
        ai_analysis,
        recognized,
        unrecognized
    ),

    trim(ai_analysis, '"') as ai_analysis,
    nullif(recognized, '') as recognized,
    nullif(unrecognized, '') as unrecognized,

from {{ ref('ext_mysql__semantic_search') }}