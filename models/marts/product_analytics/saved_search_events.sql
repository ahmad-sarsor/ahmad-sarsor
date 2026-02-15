{{ config(
    materialized='table',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "action_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['user_id', 'saved_search_id', 'saved_search_type', 'action_category'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with events as (
    select
        user_id,
        user_pseudo_id,
        session_id,
        event_id,
        event_name,
        date(event_datetime_utc) as action_date,
        event_datetime_utc as action_datetime,
        
        page_title,
        page_referrer,
        referrer_path,
        page_location,
        page_path,
        page_query,
        grouped_page_path,
        grouped_page_name,

        entity_type,
        type,
        step,

    from {{ ref('stg_ga4__events_pivoted') }}

    where event_name in (
        'saved_searches',
        'saved_searches_notifications',
        'export_search_results'
    )

    -- and user_pseudo_id = '71359602.1725881517'
    -- and user_id = 'AjpyfjtYWvoBfaD4s0RT8lUBIq1nko1mPG37nfDuJeb3axSkvnNWDe'
)
-- select * from events order by user_pseudo_id, event_datetime_utc /*

, saved_searches as (
    select *

    from {{ ref('int_saved_searches') }}
)

, sessions as (
    select
        session_id,
        session_date,
        session_duration,
        suspected_fraud,

        {{ user_dimensions() }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions() }}

    from {{ ref('sessions') }}
)

, with_parsed_id as (
    select
        *,

        regexp_extract(page_referrer, '[?&]savedsearchid=([^&]+)') as referrer_saved_search_id,
        regexp_extract(page_location, '[?&]savedsearchid=([^&]+)') as saved_search_id,

    from events
)
-- select * from ga4_union order by user_pseudo_id, event_datetime_utc /*

, events_processed as (
    select
        * except (referrer_saved_search_id, saved_search_id),

        case
            when event_name = 'saved_searches_notifications' then coalesce(
                nullif(saved_search_id, ''),
                nullif(referrer_saved_search_id, '')
            )
            when event_name = 'export_search_results' then saved_search_id
        end as saved_search_id,

        case
            when event_name = 'saved_searches'
                then case
                    when lower(type) = 'new saved search'
                        then 'create'

                    when lower(type) = 'delete search'
                        then 'delete'

                    when lower(type) = 'rename search'
                        then 'rename'

                    when lower(type) = 'copy link to share'
                        then 'share'

                    when lower(type) = 'export'
                        then 'export'
                end

            when event_name = 'saved_searches_notifications'
            and step = 'Save changes'
            and type in ('Activated', 'Deactivated')
                then 'notification'

            when event_name = 'export_search_results'
                then 'export'
        end as action_category,

        case
            when event_name = 'saved_searches_notifications'
            and type in ('Activated', 'Deactivated')
                then lower(type)
            
            when event_name = 'export_search_results' and type like 'Success%'
                then lower(split(type, ' ')[safe_offset(1)])
        end as action,

    from with_parsed_id
)
-- select * from events_processed order by user_pseudo_id, event_datetime_utc /*

select
    {{ dbt_utils.generate_surrogate_key([
        'events.user_id',
        'events.saved_search_id',
        'action',
        'action_datetime',
        'event_id'
    ]) }} as record_id,

    coalesce(events.action_date, sessions.session_date) as date_day,

    if(left(events.user_id, 1) = '_', null, events.user_id) as user_id,

    events.saved_search_id,

    saved_searches.* except (saved_search_id, user_id, saved_search_type),

    coalesce(
        saved_search_type,
        case
            when grouped_page_name like '%Multinational%'
                    then 'Multinationals'

            when grouped_page_name like '% Search'
                then split(grouped_page_name, ' ')[safe_offset(0)]

            else 'Saved Search Page'
        end
    ) as saved_search_type,

    sessions.* except (session_id, session_date),

    events.* except (user_id, saved_search_id),

from events_processed as events

left join saved_searches
    on events.saved_search_id = saved_searches.saved_search_id
    and events.user_id = saved_searches.user_id

left join sessions
    on events.session_id = sessions.session_id
    and events.action_date = sessions.session_date

where events.saved_search_id is not null
or events.page_path like '%/savedsearch%'