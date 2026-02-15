{{ config(
    materialized='incremental',
    schema=var('intermediate_schema'),
    unique_key='event_id',
    partition_by={
        "field": "date_day",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['date_day', 'user_pseudo_id', 'session_id', 'action'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}
{%- set default_tab = 'overview' -%}

with events as (
    select
        {{ ga4_user_ids() }}

        {{ ga4_event_ids() }}

        batch_sort_key,

        page_title,
        page_referrer,
        page_location,
        page_path,
        grouped_page_path,
        grouped_page_name,
        tab,

        company_url_name,

        page_view_rank,
        page_view_engagement_time_seconds,
        engagement_time_seconds,
        engagement_time_seconds_lead,
        type,

        case
            when event_name = 'page view'
                then event_name

            when event_name = 'profile_interactions'
                then case
                    when lower(section) = 'follow' then concat('follow - ', lower(entity_type))
                    when lower(entity_type) like 'claim profile %' then lower(entity_type)
                    else lower(section)
                end

            when event_name = 'add_to_collection' and lower(type) = 'profile'
                then 'profile added to list'

            when event_name = 'export_profile'
                then lower(type)

            when event_name = 'profile_external_links'
                then lower(type)

            when event_name = 'connect_request'
                then concat('connect request - ', lower(step))

        end as action,

    from {{ ref('stg_ga4__events_pivoted') }}

    where company_url_name is not null

    {{ incremental_predicate(
        history_days,
        source_column='event_date',
        target_column='date_day',
        and_=True
    ) }}

    -- where event_date = '2025-02-11'
    -- and session_id = '_5ca06fe24f446de945109039c31eeba220250211'
    -- and company_url_name = 'uveye'

    -- and user_pseudo_id = '1378043371.1703753588' and event_date = '2024-09-19' and event_id = 'bfff9429606e6c429e815ef237846d93'

    -- and event_id = '6babd1e47afe708659e6a22e14f20a4f'
    -- and (filled_user_id or filled_user_pseudo_id) and page_path = '/company_page/copyleaks' and event_date = '2024-11-18'

    -- and event_date = '2024-09-06' and event_name = 'profile_interactions'
    -- and event_date = '2024-09-23' and user_pseudo_id = '1000785994.1727076748'
    -- and page_path like '%/company_page/armis%' and user_pseudo_id = '278619715.1730418313'
    -- and page_path like '%/company_page/armis%' and user_pseudo_id = '2088743292.1726454356'
    -- and session_id = '2116249780.17248624731730433223' and page_path = '/company_page/noma-security' and event_date = '2024-11-01'
    -- and session_id = '2116249780.17248624731730433223' and page_path = '/company_page/skin-cancer-scanning' and event_date = '2024-11-01'
    -- and session_id = '1957430984.17304490721730449072' and page_path = '/company_page/mitrassist' and event_date = '2024-11-01'
    -- and page_path = '/company_page/believer-meats' and event_date = '2024-11-01'
    -- and page_path = '/company_page/aquinovo' and event_date = '2024-11-01'
    -- and page_path = '/company_page/capow-technologies' and event_date = '2024-11-01'
    -- and page_path = '/company_page/2bprecise' and event_date = '2024-11-18'
    -- and page_path = '/company_page/bondit' and event_date = '2024-11-01'
    -- and page_path = '/company_page/datarails' and event_date = '2024-11-01'
    -- and page_path = '/company_page/demostack' and event_date = '2024-11-01'
)
-- select * from events /*
-- select * from events order by session_id, event_datetime_utc /*
-- select * from events where page_view_rank is not null and action is not null order by session_id, event_datetime_utc /*

, companies as (
    select
        {# {{ company_dimensions() }} #}
        company_id,
        company_url_name,

    from {{ ref('int_companies') }}

    qualify row_number() over (partition by company_url_name order by updated_date desc) = 1
)

, merged as (
    select *

    from events

    left join companies using (company_url_name)
)

, with_action_category as (
    select
        event_date as date_day,

        if(left(user_id, 1) = '_', null, user_id) as user_id,

        * except (event_date, user_id, action, tab),
        initcap(case
            when action = 'clicks on team members' then "click on member's linkedin"
            when action = 'careerspage' then 'careers page'
            when action like 'success%' then replace(action, ' ', ' - ')
            when (type = 'Connect to Team' and action ='team') then 'connect to team'
            when action in ('similar-companies', 'suggested-companies', 'google-play', 'app-store')
                then replace(action, '-', ' ')
            else action
        end) as action,
        initcap(case
            when event_name = 'page_view' then 'page view'
            when event_name = 'connect_request' then 'connect request'
            when action = 'similar-companies' then 'tabs'
            when action = 'portfolio' then 'tabs'
            when action = 'extensions' then 'tabs'
            when action = 'overview' then 'tabs'
            when action = 'funds' then 'tabs'
            when action = 'financials' then 'tabs'
            when action = 'exits' then 'tabs'
            when action = 'about' then 'tabs'
            when action = 'team' then 'tabs'
            when action = 'news' then 'tabs'
            when action = 'lifecycle' then 'tabs'
            when action = 'israeli portfolio' then 'tabs'
            when action = 'investments' then 'tabs'
            when action = 'gallery' then 'tabs'
            when action = 'suggested-companies' then 'tabs'
            when action = 'business' then 'tabs'
            when action = 'internal' then 'tabs'
            when action = 'website' then 'external links'
            when action = 'careerspage' then 'external links'
            when action = 'youtube' then 'external links'
            when action = 'facebook' then 'external links'
            when action = 'instagram' then 'external links'
            when action = 'linkedin' then 'external links'
            when action = 'twitter' then 'external links'
            when action = 'google-play' then 'external links'
            when action = 'app-store' then 'external links'
            when action = 'read article popup news click' then 'news click'
            when action = 'news click' then 'news click'
            when action = 'funding rounds' then 'funding rounds'
            when action = 'video in profile' then 'videos and photos'
            when action = 'show_others_banner_click' then 'show others - banner click'
            when action = 'clicks on team members' then 'external links'
            when lower(action) = 'export profile' then 'export profile'
            when lower(action) like 'success%' then 'export profile'
            when lower(action) like 'claim profile%' then 'claim profile'
            when lower(action) like 'overview%' then 'overview'
            when lower(action) like 'follow%' then 'follow'
            else action
        end) as action_category,

        if(
            session_id != '{{ var("empty_session_id") }}' and company_url_name is not null,
            case
                when event_name in ('session_start', 'first_visit', 'first_open', 'page_view') then '{{ default_tab }}'
                else tab
            end,
            null
        ) as tab,

    from merged
)
-- select * from merged order by session_id, event_datetime_utc /*
-- select count(*) from merged where user_type = 'Startup' /*
-- select * from merged where action_category is null /*
-- select * from merged where action_category is null and event_name != 'page_view' /*

, with_previous_tab as (
    select
        *,

        if(
            session_id != '{{ var("empty_session_id") }}' and company_url_name is not null,
            lag(tab) over (
                partition by session_id, page_view_rank
                order by event_datetime_utc, batch_sort_key
            ),
            null
        ) as previous_tab,

    from with_action_category
)
-- select * from with_previous_tab order by session_id, event_datetime_utc /*

, with_tabs_ranking as (
    select
        *,
        
        if(
            session_id != '{{ var("empty_session_id") }}' and company_url_name is not null,
            countif(
                (tab is not null and previous_tab is null)
                or (tab != previous_tab and tab is not null and previous_tab is not null)
            ) over (
                partition by session_id, page_view_rank
                order by event_datetime_utc, batch_sort_key
                rows between unbounded preceding and current row
            ),
            null
        ) as tab_rank,

    from with_previous_tab
)
-- select * from with_tabs_ranking order by event_datetime_utc /*

, with_tabs_engagement as (
    select
        * except (tab, previous_tab, page_view_engagement_time_seconds),

        sum(engagement_time_seconds) over(
            partition by session_id, page_view_rank
        ) as page_view_ranked_engagement_time_seconds,

        last_value(tab ignore nulls) over (
            partition by session_id, page_view_rank, tab_rank
            order by event_datetime_utc, batch_sort_key
        ) as tab,

        sum(engagement_time_seconds_lead) over(
            partition by session_id, page_view_rank, tab_rank
        ) as tab_engagement_time_seconds,

    from with_tabs_ranking
)
select * from with_tabs_engagement 

/*

-- select
--     session_id, session_duration, event_id, event_name, event_datetime_utc,
--     page_title, page_path, company_url_name,
--     page_view_rank, page_view_ranked_engagement_time_seconds, engagement_time_seconds, engagement_time_seconds_lead,
--     action, action_category,
--     tab_rank, tab, tab_engagement_time_seconds
-- from with_tabs_engagement
-- order by session_id, page_view_rank, event_datetime_utc /*
/**/