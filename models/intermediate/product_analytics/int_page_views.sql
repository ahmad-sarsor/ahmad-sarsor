{{ config(
    materialized='incremental',
    schema=var('intermediate_schema'),
    unique_key='event_id',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['event_date', 'user_pseudo_id', 'session_id', 'grouped_page_name'],
    tags=['product_analytics'] 
) }}

{%- set history_days = var("incremental_window_days") -%}

with source as (
    select
        {{ ga4_user_ids() }}

        {{ ga4_event_ids() }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions(except=['channel_grouping']) }}

        event_key,
        event__string_value,
        event__int_value

    from {{ ref('stg_ga4__events_unnested') }}

    where
        event_name = 'page_view'

        and event_key in (
            'page_referrer',
            'page_location',
            'page_title'
        )

        {{ incremental_predicate(history_days, and_=True) }}
)
-- select * from source order by event_datetime_utc /*

, pivoted as (
    select
        * except (event_name, event_key, event__string_value, event__int_value),

        max(
            case when event_key = 'page_referrer' then event__string_value end
        ) as page_referrer,

        max(
            case when event_key = 'page_location' then event__string_value end
        ) as page_location,

        max(
    case 
        when event_key = 'page_title' then
        case 
            when event__string_value like '%|%' then trim(split(event__string_value, '|')[offset(0)])
            else event__string_value
        end
    end
    ) as page_title

    from source

    group by all
)

, with_url_components as (
    select
        *,

        {{ url_page_path_and_query() }}

    from pivoted
    qualify row_number() over (partition by event_id order by event_datetime_utc desc) =1 --- need to remove and check
)

select
    *,

    {{ grouped_page_path_and_name(column='referrer_path') }}

    {{ grouped_page_path_and_name(column='page_path') }}

from with_url_components
