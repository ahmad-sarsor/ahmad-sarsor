{{ config(
    materialized='incremental',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "session_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['user_signup_date', 'user_id', 'user_pseudo_id', 'session_id'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}
{%- set power_user_sessions_threshold = 25 -%}
{%- set power_user_year_threshold = 2024 -%}

with source as (
    select
        nullif(user_id, '{{ var("empty_user_id") }}') as user_id,

        nullif(user_pseudo_id, '{{ var("empty_user_pseudo_id") }}') as user_pseudo_id,

        * except (user_id, user_pseudo_id),

    from {{ ref('int_sessions') }}

    {{ incremental_predicate(
        history_days,
        source_column='session_date',
        target_column='session_date',
        where_=True
    ) }}

    -- where user_pseudo_id = '1098875088.1725631950'
    -- where session_date = '2025-01-03' and user_pseudo_id = '2005314688.1735891073'
)
-- select * from source order by session_date /*
-- select * from source where session_id = '576361405.16847947451725567246' /*
-- select session_id, count(*) c
-- from source
-- group by all
-- having c > 1
-- order by c desc /*
-- select user_pseudo_id, countif(user_id is null) n, count(distinct user_id) c
-- from source
-- group by all
-- having c > 1 and n > 0
-- order by c desc /*

, sessions_with_user_id as (
    select
        if (
            user_id is null and session_id != '{{ var("empty_session_id") }}',

            last_value(user_id ignore nulls)
                over(partition by user_pseudo_id order by session_min_event_datetime desc),
                    
            null
        ) as filled_user_id,

        *,

    from source
)
-- select * from sessions_with_user_id where user_pseudo_id = '1000296797.1681673526' /*
-- select * from sessions_with_user_id where filled_user_id is not null limit 10 /*
-- select * from sessions_with_user_id limit 10 /*

, sessions as (
    select
        coalesce(
            coalesce(user_id, filled_user_id),
            user_pseudo_id
        ) as _merge_key,

        -- filled_user_id is not null as filled_user_id,

        coalesce(user_id, filled_user_id) as user_id,

        * except (user_id, filled_user_id),

    from sessions_with_user_id
)
-- select * from sessions where user_pseudo_id = '1385918524.1690207263' order by session_min_event_datetime /*
-- select * from sessions /*

, users as (
    select
        nullif(user_id, '{{ var("empty_user_id") }}') as user_id,
        nullif(user_pseudo_id, '{{ var("empty_user_pseudo_id") }}') as user_pseudo_id,
        
        coalesce(
            nullif(user_id, '{{ var("empty_user_id") }}'),
            nullif(user_pseudo_id, '{{ var("empty_user_pseudo_id") }}')
        ) as _merge_key,

        signup_start_datetime as user_signup_start_datetime,
        signup_complete_datetime as user_signup_complete_datetime,
        user_signup_date,
        is_user_registered,
        is_user_confirmed,
        is_snc_employee,
        user_primary_usage,
        user_type,
        entity_analysis_type,
        derived_user_type,
        user_first_name,
        user_last_name,
        user_gender,
        user_email,
        is_member,
        is_password_empty,
        is_email_validated,

        {{ company_dimensions(prefix='user_') }}

    from {{ ref('int_users') }}

    qualify row_number() over(
        partition by user_id
        order by is_user_registered desc, is_user_confirmed desc, user_signup_date, signup_start_datetime
    ) = 1
)
-- select * from users where user_id = 'agxzfmlsbGlzdHNpdGVyFQsSCE5ld19Vc2VyGICAgKui95MKDA' or user_pseudo_id = '576361405.1684794745'/*
-- select * from users where user_pseudo_id = '2078454981.1652012243' /*
-- select countif(user_company_id is not null), countif(user_company_name is not null) from users /*

, merged as (
    select
        sessions.record_id,

        if(left(sessions.user_id, 1) = '_', null, sessions.user_id) as user_id,

        sessions.* except (record_id, user_id, _merge_key),

        users.* except (user_id, user_pseudo_id, _merge_key),

    from sessions

    left join users using (_merge_key)

    -- where sessions.user_id = 'agxzfmlsbGlzdHNpdGVyFQsSCE5ld19Vc2VyGICAgKui95MKDA'
    -- and sessions.user_pseudo_id = '576361405.1684794745'
    -- and sessions.session_id = '576361405.16847947451725567246'
    -- where sessions.user_pseudo_id = '1385918524.1690207263' order by session_min_event_datetime /*
)

select
    *,

    if(
        user_id is not null and extract(year from session_date) >= {{ power_user_year_threshold }},
        count(distinct session_id) over (partition by user_id)
            >= {{ power_user_sessions_threshold }},
        null
    ) as is_power_user,

from merged

/**/