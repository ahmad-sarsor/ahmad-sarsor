{{ config(
    enabled=false,
    materialized='view',
    schema=var('ga4_staging_schema'),
) }}

select
    PARSE_DATE('%Y%m%d', event_date) as event_date,
    TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
    user_id,
    user_pseudo_id,
    up.key as user_property__key,
    COALESCE(
        up.value.string_value,
        CAST(up.value.int_value as STRING),
        CAST(up.value.float_value as STRING),
        CAST(up.value.double_value as STRING)
    ) as user_property__value
from {{ source('ga4', 'events_*') }},
    UNNEST(user_properties) as up
