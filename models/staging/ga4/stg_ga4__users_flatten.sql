{{ config(
    enabled=false,
    materialized='incremental',
    schema=var('ga4_staging_schema'),
    unique_key='user_id',
    partition_by={
        "field": "last_updated_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

{% set partitions_to_replace = 3 %}

with source as (
    select *
    from {{ source('ga4', 'users_*') }}
    where
        {% if is_incremental() %}
            _table_suffix between
            FORMAT_DATE(
                '%Y%m%d',
                DATE_SUB(
                    CURRENT_DATE(), interval {{ partitions_to_replace }} day
                )
            )
            and FORMAT_DATE('%Y%m%d', CURRENT_DATE())
            and DATE((PARSE_DATE('%Y%m%d', _table_suffix))) > (
                select MAX(last_updated_date)
                from {{ this }}
            )
        {% else %}
        -- On the first run, process all available data
        _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    {% endif %}
)

select
    user_id,
    DATETIME(TIMESTAMP_MICROS(user_info.last_active_timestamp_micros), 'UTC')
        as user_info__last_active_datetime,
    case
        when user_info.user_first_touch_timestamp_micros != 0
            then
                DATETIME(
                    TIMESTAMP_MICROS(
                        user_info.user_first_touch_timestamp_micros
                    ),
                    'UTC'
                )
        else null
    end as user_info__first_touch_datetime,
    device.operating_system as device__operating_system,
    device.category as device__category,
    device.mobile_brand_name as device__mobile_brand_name,
    device.unified_screen_name as device__unified_screen_name,
    geo.continent as geo__continent,
    geo.region as geo__region,
    geo.country as geo__country,
    geo.city as geo__city,
    (
        select value.string_value
        from UNNEST(user_properties)
        where key = 'slot_01'
    ) as user_properties__user_category,
    (
        select DATETIME(TIMESTAMP_MICROS(value.set_timestamp_micros), 'UTC')
        from UNNEST(user_properties)
        where key = 'slot_01'
    ) as user_properties__user_category_set_datetime,
    (
        select value.string_value
        from UNNEST(user_properties)
        where key = 'slot_02'
    ) as user_properties__user_type,
    (
        select DATETIME(TIMESTAMP_MICROS(value.set_timestamp_micros), 'UTC')
        from UNNEST(user_properties)
        where key = 'slot_02'
    ) as user_properties__user_type_set_datetime,
    user_ltv.sessions as user_ltv__sessions,
    user_ltv.engagement_time_millis / 1000 as user_ltv__engagement_time_seconds,
    user_ltv.engagement_time_millis
    / (1000 * 60) as user_ltv__engagement_time_minutes,
    user_ltv.purchases as user_ltv__purchases,
    user_ltv.engaged_sessions as user_ltv__engaged_sessions,
    user_ltv.session_duration_micros
    / 1000000 as user_ltv__session_duration_seconds,
    user_ltv.session_duration_micros
    / (1000000 * 60) as user_ltv__session_duration_minutes,
    privacy_info.is_limited_ad_tracking as privacy_info__is_limited_ad_tracking,
    DATE(PARSE_DATE('%Y%m%d', last_updated_date)) as last_updated_date
from source
qualify
    ROW_NUMBER() over (partition by user_id order by last_updated_date desc) = 1
