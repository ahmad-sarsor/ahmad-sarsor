{{ config(
    materialized='incremental',
    schema=var('intermediate_schema'),
    unique_key='event_id',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['event_date', 'user_pseudo_id', 'user_id', 'session_id'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with source as (
    select
        nullif(user_id, '{{ var("empty_user_id") }}') as user_id,
        user_pseudo_id,
        session_id,
        event_id,
        event_name,
        event_datetime_utc,
        event_key,
        event__string_value,

    from {{ ref('stg_ga4__events_unnested') }}

    where
        event_name in ('sign_up', 'profile_data')

        and event_key in (
            'page_location',
            'page_title',
            'action',
            'platform',
            'type',
            'step',
            'origin'
        )

        {{ incremental_predicate(history_days, and_=True) }}

    -- and event_date = '2025-01-03' and user_id in (
    --     '6WvE4BcQzzFIAkem9g9jD4TDEogKYBIP7vJ5fty5YRr2QLrZ2YI4sq',
    --     'wnC8spTAGXyml1JVVuIIOXbKwNFzrG7gsVPkV9YKS3wBKPppltQl24'
    -- )
)
-- select * from source /*
-- select user_id, count(distinct if(event_key = 'step', event__string_value, null)) c from source group by 1 order by c desc /*
-- select * from source where user_id = 'EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD' or user_pseudo_id = '303113579.1725071055' order by event_datetime_utc /*
-- select * from source where user_id = 'miJOBPN0BUYcOvbBGgyZZziaWaZPKh5JT2hs6sZ30ztYL37spWYaDF' or user_pseudo_id = '643144466.1725312680' order by event_datetime_utc /*

, user_ids as (
    select
        nullif(user_pseudo_id, '{{ var("empty_user_pseudo_id") }}') as user_pseudo_id,

        array_agg(
            nullif(user_id, '{{ var("empty_user_id") }}')
            ignore nulls
            order by signup_start_datetime
            limit 1
        )[safe_offset(0)] as user_id,

    from {{ ref('int_users') }}

    group by 1
)
-- select * from user_ids where user_id = 'EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD' or user_pseudo_id = '303113579.1725071055' /*

, extracted as (
    select
        user_id,
        user_pseudo_id,
        session_id,
        event_id,
        event_datetime_utc,

        array_agg(
            case when event_key = 'page_location' then event__string_value end
            ignore nulls
            order by event_datetime_utc
            limit 1
        )[safe_offset(0)] as page_location,

        array_agg(
            case when event_key = 'page_title' then event__string_value end
            ignore nulls
            order by event_datetime_utc
            limit 1
        )[safe_offset(0)] as page_title,

        array_agg(
            case when event_key = 'origin' then event__string_value end
            ignore nulls
            order by event_datetime_utc
            limit 1
        )[safe_offset(0)] as signup_origin,

        array_agg(
            case when event_key = 'platform' then event__string_value end
            ignore nulls
            order by event_datetime_utc
            limit 1
        )[safe_offset(0)] as signup_platform,

        array_agg(
            case when event_key = 'type' then event__string_value end
            ignore nulls
            order by event_datetime_utc
            limit 1
        )[safe_offset(0)] as signup_type,

        array_agg(
            case
                when event_name = 'sign_up' then 'started'
                when event_name = 'profile_data' and event_key = 'step' then event__string_value
            end
            ignore nulls
            order by event_datetime_utc
            limit 1
        )[safe_offset(0)] as signup_step,

    from source

    group by all 
)
-- select * from extracted where user_id = 'EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD' or user_pseudo_id = '303113579.1725071055' order by event_datetime_utc /*
-- select * from extracted where user_id = 'miJOBPN0BUYcOvbBGgyZZziaWaZPKh5JT2hs6sZ30ztYL37spWYaDF' or user_pseudo_id = '643144466.1725312680' order by event_datetime_utc /*

, with_user_id as (
    select
        coalesce(extracted.user_id, user_ids.user_id) as user_id,
        extracted.* except (user_id)

    from extracted

    left join user_ids
        on extracted.user_pseudo_id = user_ids.user_pseudo_id
)
-- select * from with_user_id where user_id = 'EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD' or user_pseudo_id = '303113579.1725071055' order by event_datetime_utc /*

, with_signup_type as (
    select
        * except (signup_type),

        coalesce(
            first_value(signup_type ignore nulls) over (
                partition by user_id, signup_platform, date_trunc(event_datetime_utc, hour)
                order by event_datetime_utc desc
            ),
            'new'
        ) as signup_type,

    from with_user_id
)
-- select * from with_signup_type where user_id = 'EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD' or user_pseudo_id = '303113579.1725071055' order by event_datetime_utc /*

, deduplicated as (
    select
        *,
        date_trunc(event_datetime_utc, minute) as event_datetime_minute,
    
    from with_signup_type

    qualify row_number() over (
        partition by user_id, signup_platform, signup_type, signup_step, event_datetime_minute
        order by event_datetime_utc
    ) = 1
)
-- select * from deduplicated where user_id = 'EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD' or user_pseudo_id = '303113579.1725071055' order by event_datetime_utc /*
-- select * from deduplicated where user_id = 'bj2BbDxlYOCfezEFIBhhGXH842apZo3jkeYE6BeXLrdQh72mw4B8a2' order by event_datetime_utc /*

, with_rank as (
    select
        * except (event_datetime_minute),

        case
            when signup_step = 'started'
                then dense_rank() over (
                    partition by
                        user_id,
                        signup_platform,
                        case when signup_step = 'started' then 1 else 0 end
                    order by event_datetime_utc
                )
        end as signup_rank,

    from deduplicated
)
-- select * from with_rank where user_id = 'EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD' or user_pseudo_id = '303113579.1725071055' order by event_datetime_utc /*
-- select * from with_rank where user_id = 'bj2BbDxlYOCfezEFIBhhGXH842apZo3jkeYE6BeXLrdQh72mw4B8a2' order by event_datetime_utc /*
-- select * from with_rank where user_id = 'miJOBPN0BUYcOvbBGgyZZziaWaZPKh5JT2hs6sZ30ztYL37spWYaDF' or user_pseudo_id = '643144466.1725312680' order by event_datetime_utc /*

select
    date(event_datetime_utc) as event_date,

    * except (signup_rank),

    coalesce(
        case
            when signup_step = 'email verification sent'
                then last_value(signup_rank ignore nulls) over (partition by user_id order by event_datetime_utc desc)
            else last_value(signup_rank ignore nulls) over (partition by user_id order by event_datetime_utc)
        end,
        1
    ) as signup_attempt,

from with_rank

-- where user_id = 'miJOBPN0BUYcOvbBGgyZZziaWaZPKh5JT2hs6sZ30ztYL37spWYaDF' or user_pseudo_id = '643144466.1725312680' order by event_datetime_utc /*
-- where user_id = "t9phWp72CluVq3RUeb5iXIA5zpvX8mONYWqFYv2ZaI6dk9fhthobfb"
-- where user_id in (
--     "EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD",
--     "bj2BbDxlYOCfezEFIBhhGXH842apZo3jkeYE6BeXLrdQh72mw4B8a2"
-- )
-- order by user_id, event_datetime_utc

/**/