{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id as notification_id,
    smartlist_id,
    safe_cast(interval_days as integer) as interval_days,
    modification_updates as fields,
    nullif(datetime(last_notification), datetime('1970-01-01')) as last_notification_datetime,
    nullif(datetime(created_date), datetime('1970-01-01')) as created_datetime,

from {{ ref('ext_mysql__smartlist_notifications') }}