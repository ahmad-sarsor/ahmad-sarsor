{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id as message_id,
    nullif(channel_id, '') as channel_id,
    nullif(user_id, '') as user_id,
    nullif(status, '') as status,
    message_time as message_datetime,
    nullif(message, '') as message,

from {{ ref('ext_mysql__chat_history') }}