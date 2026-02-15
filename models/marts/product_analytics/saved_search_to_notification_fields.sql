{{ config(
    materialized='table',
    schema=var('marts_schema'),
    unique_key='record_id',
    tags=['product_analytics']
) }}

with notifications as (
    select
        notification_id,
        smartlist_id as saved_search_id,
        interval_days as notification_interval_days,
        fields as notification_fields,
        last_notification_datetime,
        created_datetime as notification_created_datetime,

    from {{ ref('stg_mysql__smartlist_notifications') }}
)
-- select count(*) from notifications /*

, notifications_unnested_fields as (
    select *

    from notifications

    left join unnest(split(notification_fields, ',')) as notification_field
)
-- select * from notifications_unnested_fields limit 10 /*

, saved_searches as (
    select
        search_id as saved_search_id,
        created_datetime as saved_search_created_datetime,
        last_modified_datetime as saved_search_last_modified_datetime,

    from {{ ref('stg_mysql__saved_searches') }}
)
-- select count(*) from saved_searches /*

, merged as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'notifications.notification_id',
            'notifications.saved_search_id',
            'notifications.notification_field',
        ]) }} as record_id,

        notifications.saved_search_id,
        saved_searches.saved_search_created_datetime,
        saved_searches.saved_search_last_modified_datetime,
        notifications.notification_id,
        notifications.notification_interval_days,
        -- notifications.notification_fields,
        notifications.notification_field,
        notifications.last_notification_datetime,
        notifications.notification_created_datetime,

    from notifications_unnested_fields as notifications
    inner join saved_searches using (saved_search_id)
)
select * from merged

-- select count(*) from merged /*
/**/