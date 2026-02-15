{{ config(
    materialized='table',
    schema=var('sheets_staging_schema'),
) }}

select distinct
    parse_date("%Y%m%d", cast(date as string)) as date_day,
    lower(country) as country,

from {{ source('sheets', 'bot_tracking') }}