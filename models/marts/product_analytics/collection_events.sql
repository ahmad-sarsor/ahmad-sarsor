{{ config(
    materialized='table',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "action_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['user_id', 'list_id', 'list_type', 'action_category'],
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
        
        -- page_view_rank,
        -- page_view_engagement_time_seconds,
        page_title,
        page_referrer,
        referrer_path,
        page_location,
        page_path,
        page_query,
        grouped_page_path,
        grouped_page_name,
        -- company_url_name,

        -- tab,
        -- view,

        type,
        step,
        section,
        element_clicked,
        action,
        label,
        
    from {{ ref('stg_ga4__events_pivoted') }}

    where (
        event_name in (
            'duplicate_smartlist',
            'public_watchlists',
            'share_smartlist',
            'smartlist_view',
            'smartlist_search_tabs',
            'smartlist_manual_order',
            'smartlists_notifications',
            'add_to_collection',
            'watchlist_interactions'
        )
        or (event_name = 'export_search_results' and grouped_page_path like '%/watchlist%')
        or (event_name = 'side_bar_clicks' and element_clicked = 'Notifications') -- click on notification in top bar event
    )

    {{ incremental_predicate(
        history_days,
        source_column='event_date',
        target_column='action_date',
        and_=True
    ) }}

    {# and event_date = '2025-02-09' and event_id in ('c05d76e6149884970415cc5ef924f844', 'b0d8c87579a855b84606baf86d2c584f') #}
)
{# select * from events /* #}

, collections as (
    select *

    from {{ ref('int_collections') }}
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

, events_processed as (
    select
        * except (
            event_name,
            type,
            step,
            section,
            element_clicked,
            action,
            label
        ),

        case
            when lower(page_location) like '%/watchlist/%' then regexp_extract(page_location, '/[Ww]atchlist/([^//?&]+)')
            when lower(page_location) like '%/smartlist/%' then regexp_extract(page_location, '/[Ss]martlist/([^//?&]+)')
        end as list_id,

        case
            when event_name = 'duplicate_smartlist' and step = 'success'
                then 'duplicate'

            when event_name = 'public_watchlists'
                then case
                    when lower(section) = 'filtering'
                        then 'filter'

                    when lower(section) like 'click on%'
                        then 'public_watchlist'

                    else 'public_watchlist'
                end

            when event_name = 'share_smartlist'
                then case
                    when step = 'copy and add to my smarlist'
                        then 'duplicate'

                    when step = 'link copied'
                        then 'share'

                    else 'share'

                end

            when event_name = 'export_search_results'
                then 'export'

            when event_name = 'smartlist_notifications'
                then 'notification'

            when event_name = 'smartlist_view'
                then 'view'

            when event_name = 'smartlist_search_tabs'
                then 'tabs'

            when event_name = 'smartlist_manual_order'
                then 'order'

            when event_name = 'watchlist_interactions'
                then case
                    when lower(action) = 'button_click'
                        then 'dots_menu'

                    when lower(action) = 'add_to_collection'
                        then 'add_to_collection'
                        
                    else lower(replace(action, '_', ' '))
                end

            when event_name = 'smartlists_notifications'
                then 'notifications'

            when event_name = 'side_bar_clicks' and element_clicked = 'Notifications'
                then 'notifications'

            else lower(event_name)
        end as action_category,

        case
            when event_name = 'duplicate_smartlist'
                then if(
                    step = 'success',
                    case
                        when page_query like '%utm_source=link_copied%'
                            then case
                                when net.host(page_referrer) = 'finder.startupnationcentral.org'
                                    then 'saved from homepage list'
                                
                                else 'saved from copied link'
                            end

                        when page_referrer is not null and net.host(page_referrer) != 'finder.startupnationcentral.org' 
                            then 'saved from public list'

                        else 'duplicated by user'
                    end,
                    event_name
                )

            when event_name = 'public_watchlists'
                then case
                    when lower(section) = 'filtering' then lower(type)

                    when lower(section) like 'click on%'
                        then replace(lower(section), 'click on ', '')

                    else event_name
                end

            when event_name = 'add_to_collection'
                then case
                    when grouped_page_path like '%/search%' then 'search page'
                    when grouped_page_path like '%_page%' then 'profile page'
                    else lower(replace(grouped_page_path, '/', '')) || ' page'
                end
            
            when event_name = 'watchlist_interactions'
                then case
                    when lower(action) in ('button_click', 'add_to_collection')
                        then lower(replace(label, '_', ' '))

                    else lower(action)
                end

            when event_name = 'smartlist_manual_order'
                then case
                    when lower(element_clicked) = 'button' then 'button'

                    else lower(coalesce(type, element_clicked))
                end

            when event_name = 'share_smartlist'
                then case
                    when step = 'copy and add to my smarlist'
                        then 'saved from copied link'

                    when type is not null then lower(type)

                    else 'watchlist'
                end

            when event_name = 'smartlists_notifications'
                then lower(coalesce(type, 'notifications'))

            when event_name = 'export_search_results'
                then lower(split(type, ' ')[safe_offset(1)])

            when event_name = 'smartlist_view'
                then lower(replace(element_clicked, ' view', ''))

            when event_name = 'smartlist_search_tabs'
                then lower(element_clicked)
            
            when event_name = 'side_bar_clicks' and element_clicked = 'Notifications'
                then 'notification icon clicked'

            else lower(event_name)
        end as action,

        case
            when event_name = 'public_watchlists'
                then case
                    when lower(section) = 'filtering' then element_clicked
                    when lower(section) like 'click on%' then element_clicked
                    else type
                end

            when event_name = 'watchlist_interactions'
                then case
                    when lower(action) = 'add_to_collection' then null
                    when lower(action) = 'button_click' then null
                    else lower(coalesce(element_clicked, label, section))
                end

            when event_name = 'export_search_results'
                then case
                    when type like 'Success%' then null
                    else lower(type)
                end

            when event_name = 'smartlists_notifications'
                then case
                    when lower(type) in ('activated', 'deactivated') then null
                    else lower(type)
                end

            when event_name = 'smartlist_manual_order'
                then case
                    when lower(type) in ('drag and drop') then null
                    else lower(type)
                end
            
            when event_name = 'smartlist_view'
                then case
                    when lower(element_clicked) = 'list view'
                    or lower(element_clicked) = 'landscape view'
                        then null
                    
                    else lower(element_clicked)
                end

            when event_name = 'smartlist_search_tabs'
                then case
                    when lower(element_clicked) in (
                        'companies', 'hubs', 'in the news',
                        'investors', 'multinationals', 'recently updated'
                    ) then null

                    else lower(element_clicked)
                end

            else element_clicked
        end as action_details,
        
    from events
)

select
    {{ dbt_utils.generate_surrogate_key([
        'events.user_id',
        'events.list_id',
        'action',
        'action_datetime',
        'event_id'
    ]) }} as record_id,

    coalesce(events.action_date, sessions.session_date) as date_day,

    if(left(events.user_id, 1) = '_', null, events.user_id) as user_id,

    events.list_id,

    collections.* except (list_id),

    sessions.* except (session_id, session_date),

    events.* except (user_id, list_id),

from events_processed as events

left join collections
    on events.list_id = collections.list_id

left join sessions
    on events.session_id = sessions.session_id
    and events.action_date = sessions.session_date

/**/