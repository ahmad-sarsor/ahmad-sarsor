{{ config(
    materialized='table',
    schema=var('intermediate_schema'),
    tags=['product_analytics']
) }}

with users as (
    select
        user_id,
        chat_id,

    from {{ ref('stg_mysql__new_user') }}
)

, members as (
    select *

    from {{ ref('stg_mysql__new_member') }}
)
-- select * from members /*

, chat_history as (
    select
        message_id,
        channel_id,
        user_id as message_user_id,
        message_datetime,

    from {{ ref('stg_mysql__chat_history') }}

    -- where channel_id = 'JSWgbuKCRxmovmj11HYXuwqnRZvQ5SPLxTz6l3B97MWUa3AQ1X1Oxu'
    -- and status = 'received'=
)
-- select * from chat_history order by message_datetime/*

, chat_invites as (
    select *

    from {{ ref('stg_mysql__chat_invitations') }}

    -- where channel_id = 'JSWgbuKCRxmovmj11HYXuwqnRZvQ5SPLxTz6l3B97MWUa3AQ1X1Oxu'
)
-- select * from chat_invites /*

, chat_invites_with_company as (
    select
        chat_invites.*,
        members.company_id as invited_company_id,

    from chat_invites
    
    left join members
        on chat_invites.invited_member_id = members.member_id
)
-- select * from chat_invites_with_company /*

, merged as (
    select
        channel_id,
        chat_invites.inviting_user_id,
        chat_invites.invited_user_id,
        users.user_id as message_user_id,
        chat_history.message_id,
        chat_history.message_datetime,

        datetime_diff(
            chat_history.message_datetime,
            chat_invites.invitation_datetime,
            second
        ) as time_since_invite_seconds,

    from chat_invites

    left join chat_history using (channel_id)

    left join users
        on chat_history.message_user_id = users.chat_id
)
-- select * from merged /*

-- exclude first automatic message (if within 10 seconds from invite)
-- datetime_diff(
--             chat_history.message_datetime,
--             chat_invites.invitation_datetime,
--             second
--         ) > 10

, channel_stats as (
    select
        channel_id,
        
        greatest(
            count(distinct case when message_user_id = inviting_user_id then message_id end)
                - 1 -- we subtract 1 message to offset the automatic message that is being sent when a connect request is sent
        , 0) as inviting_user_messages,

        count(distinct case when message_user_id = invited_user_id then message_id end) as invited_user_messages,
        min(message_datetime) as first_message_datetime,
        max(message_datetime) as last_message_datetime,

    from merged

    group by all
)
-- select * from channel_stats /*

select
    channel_id as record_id,
    *,

from chat_invites_with_company

left join channel_stats using (channel_id)

/**/