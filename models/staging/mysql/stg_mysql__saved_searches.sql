{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id as search_id,
    nullif(user_id, '') as user_id,
    nullif(`name`, '') as `name`,
    nullif(`description`, '') as `description`,
    nullif(`url`, '') as `url`,
    nullif(`type`, '') as `type`,
    nullif(datetime(created_date), datetime('1970-01-01')) as created_datetime,
    nullif(last_modified_date, datetime('1970-01-01')) as last_modified_datetime,
    nullif(last_notification, datetime('1970-01-01')) as last_notification_datetime,
    interval_days as notification_interval_days,

from {{ ref('ext_mysql__saved_searches') }}