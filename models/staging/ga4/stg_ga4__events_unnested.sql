{{ config(
    materialized='incremental',
    schema=var('ga4_staging_schema'),
    unique_key='event_params_key_id',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['event_name', 'event_key', 'user_pseudo_id', 'session_id']
) }}

{%- set history_days = var("incremental_window_days") -%}
{%- set dimensions = [
    'geo__city',
    'geo__country',
    'geo__continent',
    'geo__region',
    'geo__sub_continent',
    'device__category',
    'device__mobile_brand_name',
    'device__mobile_model_name',
    'device__mobile_marketing_name',
    'device__mobile_os_hardware_model',
    'device__operating_system',
    'device__operating_system_version',
    'device__vendor_id',
    'device__advertising_id',
    'device__language',
    'device__is_limited_ad_tracking',
    'device__time_zone_offset_seconds',
    'device__browser',
    'device__browser_version'
] -%}

with source as (
    select *

    from {{ source('ga4', 'events_*') }}

    {% if target.name == 'prod' %}
        {% if is_incremental() %}
            where _table_suffix
            between format_date('%Y%m%d', least(
                date_sub(
                    current_date(), interval {{ history_days }} day
                ),
                (select max(event_date) from {{ this }})
            ))
            and format_date('%Y%m%d', current_date())
        {% endif %}
    {% else %}
        where _table_suffix {{ var("dev_partitions") }}
    {% endif %}

    -- from `finder-353810.Analytics.events_*`
    -- where _table_suffix between '20240115' and '20240120'
    -- and user_pseudo_id = '1570942707.1705524713'
    -- and event_timestamp = 1705527064561107
)
-- select * from source order by user_pseudo_id, event_timestamp /*

, session_dates as (
    select session_id, min(session_date) as session_date

    from {{ this }}

    {% if target.name == 'prod' %}
        {% if is_incremental() %}
            where session_date
            between least(
                date_sub(
                    current_date(),
                    interval {{ history_days+7 }} day
                ),
                (select max(event_date) from {{ this }})
            )
            and current_date()
        {% endif %}
    {% else %}
        where session_date {{ var("dev_dates") }}
    {% endif %}

    group by 1
)
{# select * from session_dates /* #}

, extracted as (
    select
        * except (batch_ordering_id, batch_page_id),

        (
            select value.int_value
            from unnest(event_params)
            where key = 'ga_session_id'
        ) as ga_session_id,
        
        (
            select value.int_value
            from unnest(event_params)
            where key = 'ga_session_number'
        ) as ga_session_number,
        
        (
            select value.int_value
            from unnest(event_params)
            where key = 'session_engaged'
        ) as session_engaged,

        coalesce(
            batch_ordering_id,
            (
                select value.int_value
                from unnest(event_params)
                where key = 'batch_ordering_id'
            )
        ) as batch_ordering_id,

        coalesce(
            batch_page_id,
            (
                select value.int_value
                from unnest(event_params)
                where key = 'batch_page_id'
            )
        ) as batch_page_id,

        (
            select coalesce(value.float_value, value.int_value)
            from unnest(event_params)
            where key = 'engagement_time_msec'
        ) as engagement_time_msec,

        nullif(geo.city, '') as geo__city,
        nullif(geo.country, '') as geo__country,
        nullif(geo.continent, '') as geo__continent,
        nullif(geo.region, '') as geo__region,
        nullif(geo.sub_continent, '') as geo__sub_continent,

        device.category as device__category,
        device.mobile_brand_name as device__mobile_brand_name,
        device.mobile_model_name as device__mobile_model_name,
        device.mobile_marketing_name as device__mobile_marketing_name,
        device.mobile_os_hardware_model as device__mobile_os_hardware_model,
        device.operating_system as device__operating_system,
        device.operating_system_version as device__operating_system_version,
        device.vendor_id as device__vendor_id,
        device.advertising_id as device__advertising_id,
        device.language as device__language,
        device.is_limited_ad_tracking as device__is_limited_ad_tracking,
        device.time_zone_offset_seconds as device__time_zone_offset_seconds,

        coalesce(device.web_info.browser, device.browser) as device__browser,

        coalesce(
            device.web_info.browser_version,
            device.browser_version
        ) as device__browser_version,

        traffic_source.name as traffic_source__name,
        traffic_source.medium as traffic_source__medium,
        traffic_source.source as traffic_source__source,

        (select value.string_value from unnest(event_params) where key = 'source') as event_params__source,
        (select value.string_value from unnest(event_params) where key = 'medium') as event_params__medium,
        (select value.string_value from unnest(event_params) where key = 'campaign') as event_params__campaign,

        collected_traffic_source.manual_source as collected_traffic_source__manual_source,
        collected_traffic_source.manual_medium as collected_traffic_source__manual_medium,
        collected_traffic_source.manual_campaign_name as collected_traffic_source__manual_campaign_name,

        (select value.string_value from unnest(event_params) where key = 'page_location') as page_location,

        to_json_string(event_params) as event_params_json_string,

    from source

    -- where (
    --     select value.string_value
    --     from unnest(event_params)
    --     where key = 'page_location'
    -- ) like '%/company_page/2bprecise%'
    -- and event_date = '20241118'
)
-- select * from extracted order by event_timestamp /*
-- select * from extracted where user_pseudo_id = '2128847722.1731570650' and ga_session_id = 1731570674 order by event_timestamp /*
-- select distinct user_pseudo_id, user_id, ga_session_id from extracted /* 1735902425


, with_event_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'user_pseudo_id',
            'ga_session_id',
            'event_name',
            'event_timestamp',
            'event_bundle_sequence_id',
            'batch_page_id',
            'batch_ordering_id',
            'batch_event_index',
            'event_params_json_string'
        ]) }} as event_id,

        *,

    from extracted
)
-- select * from with_event_id /*

, marketing_parameters as (
    select
        *,
        
        regexp_extract(page_location, r'[?&]utm_source=([^&]+)') as utm_source,
        regexp_extract(page_location, r'[?&]utm_medium=([^&]+)') as utm_medium,
        regexp_extract(page_location, r'[?&]utm_campaign=([^&]+)') as utm_campaign,
        -- regexp_extract(page_location, r'[?&]utm_adgrp=([^&]+)') as utm_adgrp,
        -- regexp_extract(page_location, r'[?&]utm_ad=([^&]+)') as utm_ad,
        -- regexp_extract(page_location, r'[?&]utm_kw=([^&]+)') as utm_kw,
        -- regexp_extract(page_location, r'[?&]utm_mt=([^&]+)') as utm_mt,
        regexp_extract(page_location, r'[?&]campID=([^&]+)') as marketing_campaignid,
        regexp_extract(page_location, r'[?&]date=([^&]+)') as marketing_date,
        regexp_extract(page_location, r'[?&]cmp=([^&]+)') as marketing_company,
        regexp_extract(page_location, r'[?&]rgn=([^&]+)') as marketing_region,
        regexp_extract(page_location, r'[?&]chn=([^&]+)') as marketing_channel,
        regexp_extract(page_location, r'[?&]obj=([^&]+)') as marketing_objective,
        regexp_extract(page_location, r'[?&]dtl=([^&]+)') as marketing_details,

    from with_event_id
)

, filled_user_id as (
    select
        * except (user_id),

        if(
            user_pseudo_id is not null,
            coalesce(
                last_value(user_id ignore nulls) over (
                    partition by user_pseudo_id, ga_session_id, event_date
                    order by event_timestamp desc
                ),
                last_value(user_id ignore nulls) over (
                    partition by user_pseudo_id, event_date
                    order by event_timestamp desc
                )
            ),
            null
        ) as user_id,

        cast(event_timestamp / 1000000 as int) as event_timestamp_seconds,

        batch_page_id * 100 + batch_ordering_id * 10 + batch_event_index as batch_sort_key,

        if(
            user_id is null or user_pseudo_id is null,
            '_' || {{ dbt_utils.generate_surrogate_key(dimensions | reject('in', ['geo__region', 'geo__city'])) }},
            null
        ) as derived_id,

        nullif(concat(
            coalesce(user_id, '-'),
            coalesce(user_pseudo_id, '-'),
            coalesce(cast(ga_session_id as string), '-')
        ), '---') as derived_id_merge_key,
        
    from marketing_parameters
)
-- select * from filled_user_id /*
-- select * from filled_user_id where user_pseudo_id = '448434768.1730475668' order by event_timestamp, batch_sort_key /*
-- select * from filled_user_id where user_pseudo_id = '2128847722.1731570650' and ga_session_id = 1731570674 order by event_timestamp, batch_sort_key /*

, filled_user_id_2 as (
    select
        if(
            user_id is not null,
            user_id,
            coalesce(
                last_value(user_id ignore nulls) over (
                    partition by user_pseudo_id, ga_session_id, event_date
                    order by event_timestamp
                ),
                last_value(user_id ignore nulls) over (
                    partition by user_pseudo_id, event_date
                    order by event_timestamp
                )
            )
        ) as user_id,

        * except (user_id)

    from filled_user_id
)
-- select * from filled_user_id_2 order by event_timestamp, batch_sort_key /*
-- select * from filled_user_id_2 where user_pseudo_id = '2128847722.1731570650' and ga_session_id = 1731570674 order by event_timestamp, batch_sort_key /*

, recent_dim as (
    select
        user_id,
        user_pseudo_id,
        ga_session_id,
        derived_id,
        derived_id_merge_key,

        {{ wrap_columns(dimensions) }}

    from filled_user_id_2

    where (user_id is not null or user_pseudo_id is not null)
    and not (user_id is null and user_pseudo_id is null)

    qualify row_number() over (
        -- partition by user_id, user_pseudo_id, ga_session_id
        partition by derived_id_merge_key
        order by event_timestamp
    ) = 1
)
-- select * from recent_dim order by derived_id_merge_key /*
-- select * from recent_dim where user_pseudo_id = '2128847722.1731570650' and ga_session_id = 1731570674 /*

, events_with_ids as (
    select
        coalesce(
            filled.user_id,
            recent_dim.derived_id,
            filled.derived_id
            -- '{{ var("empty_user_id") }}'
        ) as user_id,

        filled.user_id is null
        and (recent_dim.derived_id is not null or filled.derived_id is not null)
            as is_user_id_generated,

        coalesce(
            filled.user_pseudo_id,
            recent_dim.derived_id,
            filled.derived_id
            -- '{{ var("empty_user_pseudo_id") }}'
        ) as user_pseudo_id,

        filled.user_pseudo_id is null
        and (recent_dim.derived_id is not null or filled.derived_id is not null)
            as is_user_pseudo_id_generated,

        -- filled.derived_id_merge_key,
        -- recent_dim.derived_id_merge_key,

        filled.* except (
            user_id, user_pseudo_id,
            {{ wrap_columns(dimensions, omit_comma=True) }}
        ),

        {% for dim in dimensions %}
            coalesce(
                cast(recent_dim.{{ dim }} as string),
                cast(filled.{{ dim }} as string)
            ) as {{ dim }},
        {% endfor %}

    from filled_user_id_2 as filled

    left join recent_dim using (derived_id_merge_key)
)
-- select * from events_with_ids limit 100 /*
-- select 
--     user_id, is_user_id_generated, user_pseudo_id, is_user_pseudo_id_generated, ga_session_id, event_name,
--     event_bundle_sequence_id, batch_sort_key, event_timestamp_seconds, event_timestamp,
-- from events_with_ids
-- -- where user_pseudo_id = '366408232.1730719907'
-- order by user_pseudo_id, ga_session_id, event_timestamp_seconds, batch_sort_key /*

, with_campaigns as (
    select
        *,

        case
            when utm_source is not null
                then utm_source

            when event_params__source not in (null, '(direct)', '(organic)', '(referral)')
                then event_params__source

            when traffic_source__source not in (null, '(direct)', '(organic)', '(referral)')
                then traffic_source__source

            when collected_traffic_source__manual_source not in (null, '(direct)', '(organic)', '(referral)')
                then collected_traffic_source__manual_source

            else trim(
                nullif(
                    coalesce(
                        event_params__source,
                        traffic_source__source,
                        collected_traffic_source__manual_source
                    ),
                    '(none)'
                ),
                '()'
            )
        end as marketing_source,

        case
            when utm_medium is not null
                then utm_medium
                
            when event_params__medium not in (null, '(direct)', '(organic)', '(referral)')
                then event_params__medium

            when traffic_source__medium not in (null, '(direct)', '(organic)', '(referral)')
                then traffic_source__medium

            when collected_traffic_source__manual_medium not in (null, '(direct)', '(organic)', '(referral)')
                then collected_traffic_source__manual_medium

            else trim(
                nullif(
                    coalesce(
                        event_params__medium,
                        traffic_source__medium,
                        collected_traffic_source__manual_medium
                    ),
                    '(none)'
                ),
                '()'
            )
        end as marketing_medium,

        case
            when utm_campaign is not null
                then utm_campaign
                
            when event_params__campaign not in (null, '(direct)', '(organic)', '(referral)')
                then event_params__campaign

            when traffic_source__name not in (null, '(direct)', '(organic)', '(referral)')
                then traffic_source__name

            when collected_traffic_source__manual_campaign_name not in (null, '(direct)', '(organic)', '(referral)')
                then collected_traffic_source__manual_campaign_name

            else trim(
                nullif(
                    coalesce(
                        event_params__campaign,
                        traffic_source__name,
                        collected_traffic_source__manual_campaign_name
                    ),
                    '(none)'
                ),
                '()'
            )
        end as marketing_campaign,

    from events_with_ids
)

, with_campaign_source as (
    select
        * except (marketing_source),

        case
            when contains_substr(marketing_source, 'baidu') then 'baidu'
            when contains_substr(marketing_source, 'bing') then 'bing'
            when contains_substr(marketing_source, 'calcalist') then 'calcalist'
            when contains_substr(marketing_source, 'facebook') then 'facebook'
            when contains_substr(marketing_source, 'forbes') then 'forbes'
            when contains_substr(marketing_source, 'google') then 'google'
            when contains_substr(marketing_source, 'wikipedia') then 'wikipedia'
            when contains_substr(marketing_source, 'yahoo') then 'yahoo'
            when contains_substr(marketing_source, 'yandex') then 'yandex'
            when contains_substr(marketing_source, 'huji.ac.il') then 'huji'
            when contains_substr(marketing_source, 'tau.ac.il') then 'tau'

            when marketing_source in ('chatgpt.com', 'chat.openai.com') then 'chatgpt.com'

            when lower(marketing_source) = 'finder'
            or contains_substr(marketing_source, 'finder-startupnationcentral')
            or contains_substr(marketing_source, 'findersnc')
            then 'finder'

            when marketing_source = 'sncwebsite'
            or contains_substr(marketing_source, 'sncentral')
            or contains_substr(marketing_source, 'snc_')
            then 'snc-website'

            else marketing_source
        end as marketing_source,

    from with_campaigns
)

, cte as (
    select
        event_id,
        event_name,

        event_timestamp,
        event_timestamp_seconds,
        date(parse_date('%Y%m%d', event_date)) as event_date,
        datetime(timestamp_micros(event_timestamp), 'UTC') as event_datetime_utc,
        engagement_time_msec,

        user_id,
        is_user_id_generated,

        user_pseudo_id,
        is_user_pseudo_id_generated,

        concat(
            user_pseudo_id,
            coalesce(cast(ga_session_id as string), event_date)
        ) as session_id,
        is_user_pseudo_id_generated as is_session_id_generated,

        derived_id_merge_key,
        ga_session_id,
        ga_session_number,
        session_engaged,

        event_bundle_sequence_id,
        batch_page_id,
        batch_ordering_id,
        batch_event_index,
        batch_sort_key,

        {{ wrap_columns(dimensions) }}

        privacy_info.analytics_storage as privacy_info__analytics_storage,
        privacy_info.ads_storage as privacy_info__ads_storage,
        privacy_info.uses_transient_token as privacy_info__uses_transient_token,

        -- -- debug
        -- utm_source,
        -- event_params__source,
        -- traffic_source__source,
        -- collected_traffic_source__manual_source,
        -- utm_medium,
        -- event_params__medium,
        -- traffic_source__medium,
        -- collected_traffic_source__manual_medium,

        marketing_source,
        marketing_medium,
        marketing_campaign,
        marketing_campaignid,
        marketing_date,
        marketing_company,
        marketing_region,
        marketing_channel,
        marketing_details,

        coalesce(
            marketing_objective,
            campaign_objectives.objective
        ) as marketing_objective,

        concat(
            coalesce(
                marketing_objective,
                campaign_objectives.objective
            ),
            ' ',
            marketing_source
        ) as marketing_campaign_group,

        stream_id,
        platform,
        is_active_user,

        event_params,
        event_params_json_string,

    from with_campaign_source
    
    left join {{ ref('stg_sheets__campaign_objectives') }} as campaign_objectives
        on with_campaign_source.marketing_campaign = campaign_objectives.campaign
)
-- select * from cte where session_id = '_5ca06fe24f446de945109039c31eeba220250211' order by event_datetime_utc /*

, fixed_session_id as (
    select
        * except (session_id),

        -- session_id, event_datetime_utc, event_name,

        case
            when is_session_id_generated then
                concat(
                    user_pseudo_id,
                    "_",
                    event_date,
                    "_",
                    cast(countif(event_name = 'session_start') over (
                        partition by session_id
                        order by session_id, event_datetime_utc, if(event_name = 'session_start', 0, 1)
                        rows between unbounded preceding and current row
                    ) as string)
                )
            else session_id
        end as session_id,

    from cte
)
-- select * from fixed_session_id where session_id = '_5ca06fe24f446de945109039c31eeba220250211' order by event_datetime_utc/*

, with_session_date as (
    select
        fixed_session_id.* except (ga_session_number, session_engaged),

        coalesce(
            session_dates.session_date,
            min(event_date) over (partition by session_id)
        ) as session_date,

        coalesce(min(ga_session_number) over (partition by session_id), 1) as ga_session_number,
        coalesce(max(session_engaged) over (partition by session_id), 0) as session_engaged,

    from fixed_session_id

    left join session_dates using (session_id)
)
{# select * from with_session_date where session_id = '_5ca06fe24f446de945109039c31eeba2_2025-02-11_1' order by event_datetime_utc /* #}

, unnested as (
    select
        event_id,

        {{ dbt_utils.generate_surrogate_key([ 
            'user_id', 
            'user_pseudo_id',
            'ga_session_id',
            'event_name',
            'event_timestamp',
            'event_bundle_sequence_id',
            'batch_page_id',
            'batch_ordering_id',
            'batch_event_index',
            'event_params_json_string',
            'ep.key',
        ]) }} as event_params_key_id,

        with_session_date.* except (event_id, engagement_time_msec, event_params, event_params_json_string),

        ep.key as event_key,

        max_by(ep.value.string_value, event_timestamp) as event__string_value,

        case
            when ep.key = 'engagement_time_msec' then sum(engagement_time_msec)
            else max_by(ep.value.int_value, event_timestamp)
        end as event__int_value,
        
        max_by(
            coalesce(ep.value.double_value, ep.value.float_value),
            event_timestamp
        ) as event__double_value,

    from with_session_date
        , unnest(event_params) as ep

    group by all
)

select *
from unnested




-- where event_timestamp = 1705527064561107
-- where event_params_key_id = '35a0dae7941c6fe75060c329bc383380'

-- select * from cte
-- where user_pseudo_id = '303113579.1725071055' and batch_page_id = 1725071050165 and batch_ordering_id = 4 and event_bundle_sequence_id = 1774905295

-- where traffic_source__source like '%direct%' or utm_source like '%direct%' or traffic_source__medium like '%direct%' or utm_medium like '%direct%'
-- where marketing_source like '%direct%'
-- order by event_datetime_utc

/**/