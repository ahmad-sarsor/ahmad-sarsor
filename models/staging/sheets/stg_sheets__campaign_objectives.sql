{{ config(
    materialized='table',
    schema=var('sheets_staging_schema'),
) }}

select distinct
    campaign,
    objective,

from {{ source('sheets', 'campaign_objectives') }}