{{ config(
    materialized='incremental',
    schema=var('intermediate_schema'),
    unique_key='record_id',
    partition_by={
        "field": "session_date",
        "data_type": "date",
        "granularity": "day"
    },
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with events as (
    select
        {{ ga4_user_ids() }}

        {{ ga4_event_ids() }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions(except=['channel_grouping']) }}

        -- session_engaged,

        event_key,
        event__string_value,
        event__int_value,

    from {{ ref('stg_ga4__events_unnested') }}

    where session_id != '{{ var("empty_session_id") }}'

    {{ incremental_predicate(
        history_days,
        source_column='event_date',
        target_column='session_date',
        and_=True
    ) }}

    -- and event_date = '2025-01-03' and user_id in (
    --     '6WvE4BcQzzFIAkem9g9jD4TDEogKYBIP7vJ5fty5YRr2QLrZ2YI4sq',
    --     'wnC8spTAGXyml1JVVuIIOXbKwNFzrG7gsVPkV9YKS3wBKPppltQl24'
    -- )
)
-- select * from events /*
-- select * from events limit 10 /*
-- select distinct user_id, user_pseudo_id, session_id from events /*

, suspected_fraud as (
    select
        date_day,
        country
    
    from {{ ref('stg_sheets__bot_tracking') }}
)

, session_metrics as (
    select
        user_pseudo_id,
        
        array_agg(
            nullif(user_id, '{{ var("empty_user_id") }}')
            ignore nulls
            order by event_datetime_utc
            limit 1
        )[safe_offset(0)] as user_id,

        session_id,
        -- max_by(ga_session_number, event_datetime_utc) as ga_session_number,

        -- max_by(is_new_user, event_datetime_utc) as is_new_user,
        -- max_by(is_returning_user, event_datetime_utc) as is_returning_user,

        logical_or(
            event_name = 'first_visit'
            or event_name = 'first_open'
        ) as is_new_user,

        not logical_or(
            event_name = 'first_visit'
            or event_name = 'first_open'
        ) as is_returning_user,
        
        session_date,

        min(event_datetime_utc) as session_min_event_datetime,

        max(event_datetime_utc) as session_max_event_datetime,

        timestamp_diff(max(event_datetime_utc), min(event_datetime_utc), second) as session_duration,

        coalesce(sum(case
            when event_key = 'engagement_time_msec'
            then event__int_value
        end) / 1000, 0) as session_engagement_time,
        
        -- coalesce(max(session_engaged), 0) as session_engaged,

        -- min(case
        --     when
        --         event_name = 'sign_up'
        --         and event_key = 'action'
        --         and event__string_value = 'Sign Up'
        --     then events.event_datetime_utc
        -- end) as signup_datetime,

        min_by(case
            when
                event_name = 'session_start'
                and event_key = 'page_location'
            then event__string_value
        end, event_datetime_utc) as landing_page
    
    from events

    group by all

    -- having session_duration > interval 0 second
)
-- select * from session_metrics /*

, session_events_funnel as (
    select
        session_id,
        string_agg(event_name order by event_datetime_utc) as events,
        -- array_agg(event_name order by event_datetime_utc) as events,
        -- array_agg(page_location order by event_datetime_utc) as page_locations,
    
    from (
        select distinct
            event_id,
            session_id,
            event_datetime_utc,
            event_name,

            -- case
            --     when event_key = 'page_location' then event__string_value
            -- end as page_location,

        from events
        join session_metrics using (session_id)
    )

    -- where session_id != '{{ var("empty_session_id") }}'

    group by 1
)

, session_dimensions as (
    select
        session_id,

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions(except=['channel_grouping']) }}
        
        {{ channel_grouping() }}

        {{ snc_region('geo__country') }}

        {{ snc_target('geo__country') }}

        geo__country = 'Israel' as israeli,

    from events
    join session_metrics using (session_id)

    -- where session_id != '{{ var("empty_session_id") }}'

    qualify row_number() over (
        partition by session_id
        order by
            if(lower(channel_grouping) like '%paid%', 1, 2),
            session_duration desc,
            event_datetime_utc desc
    ) = 1
)

select
     {{ dbt_utils.generate_surrogate_key([
        'session_metrics.session_id',
        'session_metrics.session_date'
    ]) }} as record_id, 
    --session_metrics.session_id as record_id,

    session_metrics.*,

    session_events_funnel.events as session_events,

    session_dimensions.* except (session_id),

    suspected_fraud.date_day is not null and suspected_fraud.country is not null as suspected_fraud,

from session_metrics
left join session_dimensions using (session_id)
left join session_events_funnel using (session_id)

left join suspected_fraud
    on session_metrics.session_date = suspected_fraud.date_day
    and coalesce(lower(session_dimensions.geo__country), 'unknown') = suspected_fraud.country

/**/