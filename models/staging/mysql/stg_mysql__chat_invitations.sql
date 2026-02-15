{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id as invite_id,
    nullif(channel_id, '') as channel_id,
    nullif(inviting_user_id, '') as inviting_user_id,
    nullif(invited_user_id, '') as invited_user_id,
    nullif(invited_member_id, '') as invited_member_id,
    invitation_date as invitation_datetime,
    last_reminder as last_reminder_datetime,
    reminders_num as reminders,
    nullif(status, '') as status,
    status_date as status_datetime,
    nullif(status_message, '') as status_message,
    nullif(purpose, '') as purpose,
    -- nullif(message, '') as message,
    -- nullif(substitute_invited_user_id, '') as substitute_invited_user_id,

from {{ ref('ext_mysql__chat_invitations') }}