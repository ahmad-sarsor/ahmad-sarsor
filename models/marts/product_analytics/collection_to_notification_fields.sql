{{ config(
    materialized='table',
    schema=var('marts_schema'),
    unique_key='record_id',
    tags=['product_analytics']
) }}

with notifications as (
    select
        notification_id,
        smartlist_id as list_id,
        interval_days as notification_interval_days,
        trim(fields, ',') as notification_fields,
        last_notification_datetime,
        created_datetime as notification_created_datetime,

    from {{ ref('stg_mysql__smartlist_notifications') }}

    {# where notification_id = 'FYcVFlRlHahQMrYFd2GzSttFqFeJyTJwYEcnOblt6rE7OHjvp3Sf19'
    or smartlist_id = 'KXimYz9vYj2ANgQ4ZZxmsB27zARB0hBUfVgTiSrXG6gQ5zwyWmcCGK' #}
)
{# select count(*) from notifications /* #}
{# select * from notifications /* #}

, notifications_unnested_fields as (
    select *

    from notifications

    left join unnest(split(notification_fields, ',')) as notification_field
)
{# select * from notifications_unnested_fields /* #}

, collections as (
    select
        collection_id as list_id,
        created_datetime as list_created_datetime,
        last_modified_datetime as list_last_modified_datetime,

    from {{ ref('stg_mysql__collection') }}
    
    {# where collection_id = 'KXimYz9vYj2ANgQ4ZZxmsB27zARB0hBUfVgTiSrXG6gQ5zwyWmcCGK' #}
)
{# select count(*) from collections /* #}
{# select * from collections /* #}

, merged as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'notifications.notification_id',
            'notifications.list_id',
            'notifications.notification_field',
        ]) }} as record_id,

        notifications.list_id,
        collections.list_created_datetime,
        collections.list_last_modified_datetime,
        notifications.notification_id,
        notifications.notification_interval_days,
        -- notifications.notification_fields,
        notifications.notification_field,
        notifications.last_notification_datetime,
        notifications.notification_created_datetime,

    from notifications_unnested_fields as notifications
    inner join collections using (list_id)
)
select * from merged

-- select count(*) from merged /*
/**/