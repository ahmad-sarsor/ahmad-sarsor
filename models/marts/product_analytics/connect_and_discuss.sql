{{ config(
    materialized='table',
    schema=var('marts_schema'),
    tags=['product_analytics']
) }}

{%- set excluded_company_dimensions = [[
    'user_company_id',
    'user_company_name',
    'user_company_url_name',
    'user_company_type',
    'user_company_subtype',
    'user_company_primary_sector',
    'user_company_primary_sector_parent',
]] -%}
{%- set user_dims = var("user_dimensions") | reject('in', excluded_company_dimensions) | list -%}
{%- set dimensions = user_dims + var("ga4_event_dimensions") + var("ga4_marketing_dimensions") -%}

with users as (
    select
        user_id,

        {{ user_dimensions(except=excluded_company_dimensions) }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions() }}

    from {{ ref('sessions') }}

    where user_id is not null

    qualify row_number() over (
        partition by user_id
        order by is_user_registered desc, user_signup_date, user_signup_start_datetime
    ) = 1
)
-- select * from users where user_id = 'agxzfmlsbGlzdHNpdGVyFQsSCE5ld19Vc2VyGICA4Kn-nqQIDA' /*
-- select * from users where user_id = 'agxzfmlsbGlzdHNpdGVyFQsSCE5ld19Vc2VyGICAgJ73wrAJDA' /*
-- select * from users where user_id = 'W6pDrZWFolDI1jbEo2uEGFoWipwaMr8x4WpH6ZL3vSEyWDuiimkSwT' /*

, companies as (
    select
        {{ company_dimensions() }}

    from {{ ref('int_companies') }}
)

, ga4_source as (
    select
        event_id,
        event_datetime_utc,
        user_id as inviting_user_id,
        company_url_name,
        step,
        -- entity_type,
        -- user_pseudo_id,
        -- session_id,
        -- page_location,
        -- page_title,

        -- invited_user_company.company_id as invited_user_company_id,
        -- invited_user_company.company_name as invited_user_company_name,
        -- invited_user_company.company_type as invited_user_company_type,
        -- invited_user_company.company_subtype as invited_user_company_subtype,

        countif(step = 'Button Clicked') over (
            partition by user_id, company_url_name
            order by event_datetime_utc
        ) as rank,

    from {{ ref('int_connect_request') }}

    -- left join companies as invited_user_company
    --     on source.company_url_name = invited_user_company.company_url_name

    -- where session_id = '1971936875.17231437891739373071'
    -- and event_datetime_utc between '2025-02-12T15:25:00' and '2025-02-12T15:26:00'

    -- where user_id = 'agxzfmlsbGlzdHNpdGVyFQsSCE5ld19Vc2VyGICA4Kn'
    -- where
    --     source.event_date = '2024-08-10'
    --     and source.user_id in (
    --         'GPT2olv7JCqRSix6swff0M4Qpp2TgPGgKDvadOZ2K5idGnRxK8wluO'
    -- --         'zLfTlMVzR1WMQWg6TPKMj9ey55GBdAzFmVrLPdjHTwTq4caOmSYifX',
    -- --         'dDNXBYlj44cSDReebBuCk6UULeFfsRUETNE97NAlnL3aaMwtIXrFGP',
    -- --         'fvVQDRbdgGAgcsmlToLy5lEe2mr5LhkkMvweMIWbchdlrkICAqmGZm'
    --     )
)
-- select * from ga4_source order by event_datetime_utc /*
-- select inviting_user_id, count(*) from ga4_source group by all order by 2 desc /*

, ga4_flat as (
    select
        min_by(event_id, event_datetime_utc) as record_id,

        inviting_user_id,
        company_url_name,

        min(case when step = 'Button Clicked' then event_datetime_utc end) as step__button_clicked__min_datetime,
        min(case when step = 'Success' then event_datetime_utc end) as step__sent_request_success__min_datetime,

        -- min_by(entity_type, event_datetime_utc) as entity_type,

        -- invited_user_company_id,
        -- invited_user_company_name,
        -- min_by(invited_user_company_type, event_datetime_utc) as invited_user_company_type,
        -- min_by(invited_user_company_subtype, event_datetime_utc) as invited_user_company_subtype,

        max(rank) as clicked_connect_counter,

    from ga4_source

    group by all
)
-- select * from ga4_flat order by inviting_user_id, company_url_name /*

, ga4_with_rounded_time as (
    select
        *,
        
        TIMESTAMP_MILLIS( CAST(FLOOR(
            UNIX_MILLIS(TIMESTAMP(step__sent_request_success__min_datetime)) / 10000
        ) AS INT64) * 10000) AS nearest_10seconds,

        TIMESTAMP_MILLIS( CAST(FLOOR(
            UNIX_MILLIS(TIMESTAMP(step__sent_request_success__min_datetime)) / 60000
        ) AS INT64) * 60000) AS nearest_min,

    from ga4_flat
)
-- select * from ga4_with_rounded_time order by inviting_user_id, company_url_name /*

, ga4_with_company as (
    select
        ga4.* except (company_url_name),

        companies.company_id as invited_company_id,
        ga4.company_url_name as invited_company_url_name,
        companies.company_name as invited_company_name,
        companies.company_type as invited_company_type,
        companies.company_subtype as invited_company_subtype,
        companies.company_primary_sector as invited_company_primary_sector,
        companies.company_primary_sector_parent as invited_company_primary_sector_parent,


    from ga4_with_rounded_time as ga4

    left join companies
        on ga4.company_url_name = companies.company_url_name
)
-- select * from ga4_with_company order by inviting_user_id, company_url_name /*

, mysql_source as (
    select
        invite_id,
        channel_id,
        inviting_user_id,
        invited_user_id,
        invited_company_id,
        invitation_datetime,
        TIMESTAMP_MILLIS( CAST(FLOOR(UNIX_MILLIS(TIMESTAMP(invitation_datetime)) / 10000) AS INT64) * 10000) AS nearest_10seconds,
        TIMESTAMP_MILLIS( CAST(FLOOR(UNIX_MILLIS(TIMESTAMP(invitation_datetime)) / 60000) AS INT64) * 60000) AS nearest_min,
        status,
        status_datetime,
        reminders,
        last_reminder_datetime,
        if(status = 'accepted', timestamp_diff(status_datetime, invitation_datetime, second), null) as time_to_accept_seconds,
        if(status = 'withdrawn', timestamp_diff(status_datetime, invitation_datetime, second), null) as time_to_withdraw_seconds,
        purpose,
        inviting_user_messages,
        invited_user_messages,
        first_message_datetime,
        last_message_datetime,

    from {{ ref('int_connect_and_discuss') }}

    -- where channel_id in ('uSBVjIak2kwGYYQokkhmZT7Gma1HGKIy6s0mqiVKCCQXqYUlZCCcl9', 'gj8OahJrYKRxFKRRN5jlA7vnsnlPMsjA64zaIYhFAyQ5b8UfBf1EVu')
    -- where
    --     date(invitation_datetime) = '2024-08-10'
    --     and inviting_user_id in (
    --         'GPT2olv7JCqRSix6swff0M4Qpp2TgPGgKDvadOZ2K5idGnRxK8wluO'
    --         'zLfTlMVzR1WMQWg6TPKMj9ey55GBdAzFmVrLPdjHTwTq4caOmSYifX',
    --         'dDNXBYlj44cSDReebBuCk6UULeFfsRUETNE97NAlnL3aaMwtIXrFGP',
    --         'fvVQDRbdgGAgcsmlToLy5lEe2mr5LhkkMvweMIWbchdlrkICAqmGZm'
        -- )
)
-- select * from mysql_source /*
-- select *
-- from mysql_source as mysql
-- left join ga4_with_rounded_time as ga4
--     on mysql.inviting_user_id = ga4.inviting_user_id
--     and mysql.nearest_min = ga4.nearest_min
--     and timestamp_diff(mysql.nearest_10seconds, ga4.nearest_10seconds, second) <= 10
-- order by mysql.inviting_user_id, mysql.invitation_datetime /*

, mysql_with_company as (
    select
        mysql.*,

        mysql.invited_company_id,
        companies.company_url_name as invited_company_url_name,
        companies.company_name as invited_company_name,
        companies.company_type as invited_company_type,
        companies.company_subtype as invited_company_subtype,
        companies.company_primary_sector as invited_company_primary_sector,
        companies.company_primary_sector_parent as invited_company_primary_sector_parent,

    from mysql_source as mysql

    left join companies
        on mysql.invited_company_id = companies.company_id
)
-- select * from mysql_with_company /*

, merged as (
    select
        coalesce(mysql.channel_id, ga4.record_id) as record_id,
        mysql.channel_id,
        mysql.invite_id,
        coalesce(mysql.inviting_user_id, ga4.inviting_user_id) as inviting_user_id,

        mysql.invited_user_id,
        coalesce(mysql.invited_company_url_name, ga4.invited_company_url_name) as invited_company_url_name,
        coalesce(mysql.invited_company_name, ga4.invited_company_name) as invited_company_name,
        coalesce(mysql.invited_company_type, ga4.invited_company_type) as invited_company_type,
        coalesce(mysql.invited_company_subtype, ga4.invited_company_subtype) as invited_company_subtype,
        coalesce(mysql.invited_company_primary_sector, ga4.invited_company_primary_sector) as invited_company_primary_sector,
        coalesce(mysql.invited_company_primary_sector_parent, ga4.invited_company_primary_sector_parent) as invited_company_primary_sector_parent,

        ga4.clicked_connect_counter,
        ga4.step__button_clicked__min_datetime as clicked_connect_datetime,
        coalesce(mysql.invitation_datetime, ga4.step__sent_request_success__min_datetime) as invitation_datetime,
        mysql.status,
        mysql.status_datetime,
        mysql.purpose,
        coalesce(mysql.reminders, 0) as reminders,
        mysql.last_reminder_datetime,
        -- mysql.time_to_accept_seconds,
        round(mysql.time_to_accept_seconds / 86400, 2) as time_to_accept_days,
        -- mysql.time_to_withdraw_seconds,
        round(mysql.time_to_withdraw_seconds / 86400, 2) as time_to_withdraw_days,
        mysql.inviting_user_messages,
        mysql.invited_user_messages,
        mysql.first_message_datetime,
        mysql.last_message_datetime,

    from mysql_with_company as mysql

    full outer join ga4_with_company as ga4
        on mysql.inviting_user_id = ga4.inviting_user_id
        and mysql.invited_company_url_name = ga4.invited_company_url_name
        and mysql.nearest_min = ga4.nearest_min
        and timestamp_diff(mysql.nearest_10seconds, ga4.nearest_10seconds, second) <= 10
)
-- select * from merged order by inviting_user_id, invitation_datetime /*

select
    merged.* except (
        inviting_user_id,
        invited_user_id
        -- invited_user_company_name,
        -- invited_user_company_type,
        -- invited_user_company_subtype
    ),
    
    if(left(inviting_user_id, 1) = '_', null, inviting_user_id) as inviting_user_id,
    if(left(invited_user_id, 1) = '_', null, invited_user_id) as invited_user_id,

    {#{% for dim in dimensions -%}
        inviting_user.{{ dim }} as inviting_user_{{ dim.removeprefix('user_') }},
    {% endfor %}#}

    {{ ga4_event_dimensions(dimensions=dimensions, prefix='inviting_user_', table='inviting_user') }}
    {{ ga4_event_dimensions(dimensions=dimensions, prefix='invited_user_', table='invited_user') }}

    {#
    coalesce(merged.invited_user_company_name, invited_user.user_company_name) as invited_user_user_company_name,
    coalesce(merged.invited_user_company_type, invited_user.user_company_type) as invited_user_user_company_type,
    coalesce(merged.invited_user_company_subtype, invited_user.user_company_subtype) as invited_user_user_company_subtype,

    -- {% for dim in dimensions | reject('in', ['user_company_name', 'user_company_type', 'user_company_subtype']) -%}
    --     invited_user.{{ dim }} as invited_user_{{ dim.removeprefix('user_') }},
    -- {% endfor %}

    {{ ga4_event_dimensions(dimensions=dimensions | reject('in', ['user_company_name', 'user_company_type', 'user_company_subtype']), prefix='invited_user_', table='invited_user') }}
    #}

from merged

left join users as inviting_user
    on merged.inviting_user_id = inviting_user.user_id

left join users as invited_user
    on merged.invited_user_id = invited_user.user_id

/**/