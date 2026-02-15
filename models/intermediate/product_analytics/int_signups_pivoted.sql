{{ config(
    materialized='incremental',
    schema=var('intermediate_schema'),
    unique_key='record_id',
    partition_by={
        "field": "signup_start_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['signup_start_date', 'user_id', 'signup_platform', 'signup_type'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with source as (
    select
        user_id,
        user_pseudo_id,
        session_id,
        page_location,
        page_title,
        signup_origin,
        signup_platform,
        signup_type,
        signup_attempt,
        signup_step,
        event_datetime_utc,

        min(event_datetime_utc) over (
            partition by user_id, session_id, signup_platform, signup_type
            order by event_datetime_utc
        ) as min_event_datetime
        
    from {{ ref('int_signups') }}

    {{ incremental_predicate(
        history_days,
        source_column='event_date',
        target_column='signup_start_date',
        where_=True
    ) }}

    -- where user_id = 'miJOBPN0BUYcOvbBGgyZZziaWaZPKh5JT2hs6sZ30ztYL37spWYaDF' or user_pseudo_id = '643144466.1725312680'
    -- and user_id in (
    --     "EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD",
    --     "bj2BbDxlYOCfezEFIBhhGXH842apZo3jkeYE6BeXLrdQh72mw4B8a2",
    --     "t9phWp72CluVq3RUeb5iXIA5zpvX8mONYWqFYv2ZaI6dk9fhthobfb"
    -- )

    -- where event_date = '2025-01-03' and user_id in (
    --     '6WvE4BcQzzFIAkem9g9jD4TDEogKYBIP7vJ5fty5YRr2QLrZ2YI4sq',
    --     'wnC8spTAGXyml1JVVuIIOXbKwNFzrG7gsVPkV9YKS3wBKPppltQl24'
    -- )
)
-- select * from source /*

, pivoted as (
    select *

    from source

    pivot (
        min(event_datetime_utc) as step_datetime_utc_

        for signup_step in (
            'email verification sent' as email_verification_sent,
            'started',
            'name',
            'primary usage' as primary_usage,
            'business email' as business_email,
            'complete skip' as complete_skip,
            'complete submit' as complete_submit
        )
    )
)

select
    {{ dbt_utils.generate_surrogate_key([
        'user_id',
        'signup_platform',
        'signup_type',
        'signup_attempt'
    ]) }} as record_id,

    user_id,
    signup_platform,
    signup_type,
    signup_attempt,

    array_agg(
        signup_origin
        ignore nulls
        order by min_event_datetime
        limit 1
    )[safe_offset(0)] as signup_origin,

    max(session_id) as max_session_id,

    min_by(page_location, min_event_datetime) as page_location,

    min_by(page_title, min_event_datetime) as page_title,

    date(min(min_event_datetime)) as signup_start_date,

    min(min_event_datetime) as signup_start_datetime,

    min(least(
        coalesce(step_datetime_utc__complete_submit, step_datetime_utc__complete_skip),
        coalesce(step_datetime_utc__complete_skip, step_datetime_utc__complete_submit)
    )) as signup_complete_datetime,

    min(step_datetime_utc__email_verification_sent) as signup_step__email_verification_sent__datetime_utc,

    min(step_datetime_utc__started) as signup_step__started__datetime_utc,

    min(step_datetime_utc__name) as signup_step__name__datetime_utc,

    min(step_datetime_utc__primary_usage) as signup_step__primary_usage__datetime_utc,

    min(step_datetime_utc__business_email) as signup_step__business_email__datetime_utc,

    min(step_datetime_utc__complete_skip) as signup_step__complete_skip__datetime_utc,

    min(step_datetime_utc__complete_submit) as signup_step__complete_submit__datetime_utc,

from pivoted

group by all

-- order by user_id, signup_start_datetime

/**/