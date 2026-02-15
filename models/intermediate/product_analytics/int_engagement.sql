{{ config(
    materialized='incremental',
    schema=var('intermediate_schema'),
    unique_key='event_id',
    partition_by={
        "field": "date_day",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['user_pseudo_id', 'user_id', 'session_id', 'page_location'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with source as (
    select
        {{ ga4_user_ids() }}

        event_date as date_day,
        event_id,
        event_key,
        event__int_value,
        event__double_value,
        event__string_value

    from {{ ref('stg_ga4__events_unnested') }}

    where
        event_key in ('page_location', 'page_title', 'engagement_time_msec')

        and session_id != '{{ var("empty_session_id") }}'

        {{ incremental_predicate(
            history_days,
            source_column='event_date',
            target_column='date_day',
            and_=True
        ) }}
)

,aggregated as(
    select
    * except (event_key, event__int_value, event__double_value, event__string_value),
    
    coalesce(
        min_by(case
            when event_key = 'page_location'
            then event__string_value
        end, date_day),
        '/'
    ) as page_location,
    
    coalesce(
        min_by(case
            when event_key = 'page_title'
            then event__string_value
        end, date_day),
        ''
    ) as page_title,
    
    coalesce(
        sum(case
            when event_key = 'engagement_time_msec'
            then coalesce(event__double_value, event__int_value)
        end) / 1000,
        0
    ) as engagement_time_seconds,

from source
group by user_id, user_pseudo_id, session_id, date_day, event_id
)

select *
from aggregated
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY user_id DESC) = 1