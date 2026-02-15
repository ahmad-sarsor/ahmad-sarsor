{{ config(
    materialized='incremental',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "date_day",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['session_id', 'grouped_page_path', 'page_path', 'page_location'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with source as (
    select *

    from {{ ref('bi_page_views') }}

    {{ incremental_predicate(
        history_days,
        source_column='date_day',
        target_column='date_day',
        where_=True
    ) }}
)
{# select * from source /* #}

-- flat multiple user ids to a single user id per session
, aggregated as (
    select
        * except (page_view_count, engagement_time_seconds, min_event_datetime_utc),

        sum(page_view_count) as page_view_count,
        min(min_event_datetime_utc) as min_event_datetime_utc,
        max(engagement_time_seconds) as engagement_time_seconds,

    from source

    group by all
)

, sessions as (
    select
        session_id,
        session_date as date_day,
        session_duration,
        suspected_fraud,
        
        {{ user_dimensions() }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions() }}

    from {{ ref('sessions') }}
)

select
    date_day as session_date,
    *

from aggregated

left join sessions using (session_id, date_day)