{{ config(
    materialized='view',
    schema=var('intermediate_schema'),
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}
{%- set event_name = 'connect_request' -%}
{%- set event_keys = [
    'entity_type',
    'step'
] -%}

select
    {{ ga4_user_ids() }}

    {{ ga4_event_ids() }}

    {{ ga4_event_dimensions() }}

    {{ marketing_dimensions(except=['channel_grouping']) }}

    page_title,
    page_referrer,
    page_location,

    company_url_name,

    {{ event_keys | join(',\n   ') }}

from {{ ref('stg_ga4__events_pivoted') }}

where event_name = '{{ event_name }}'