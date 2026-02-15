{{ config(
    materialized='incremental',
    schema=var('ga4_staging_schema'),
    unique_key='event_id',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['event_date', 'user_pseudo_id', 'user_id', 'session_id']
) }}

{%- set history_days = var("incremental_window_days") -%}
{%- set event_keys = [
    'page_title',
    'page_referrer',
    'page_location',
    'section',
    'entity_type',
    'entity_name',
    'type',
    'step',
    'filters',
    'element_clicked',
    'search_term',
    'action',
    'label'
] -%}

with source as (
    select
        {{ ga4_user_ids() }}

        is_user_id_generated,
        is_user_pseudo_id_generated,

        {{ ga4_event_ids() }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions(except=['channel_grouping']) }}

        batch_sort_key,

        round(if(
            event_key = 'engagement_time_msec',
            coalesce(event__double_value, event__int_value),
            null
        ) / 1000, 4) as engagement_time_seconds,

        event_key,
        event__string_value

    from {{ ref('stg_ga4__events_unnested') }}

    {{ incremental_predicate(history_days, where_=True) }}

    -- where event_date = '2025-03-16'
    -- and event_id = 'a02aa8c1ad25d2ed4c43b92cf5d0ceaa'
    
    -- where event_id = '329bc7bdcf6d9788e86133dbf4173408'

    -- where event_date = '2024-09-23' and user_pseudo_id = '1000785994.1727076748'
    -- where session_id = '584376.16630080581726470549'
    -- where ga_session_id = 1725137620
    -- where event_date = '2024-09-23' and user_pseudo_id = '1000785994.1727076748'
    -- and user_pseudo_id = '2088743292.1726454356'

    -- where event_date = '2024-11-18'
    -- and page_location = '/company_page/copyleaks'
    -- and (filled_user_id or filled_user_pseudo_id)

    -- and session_id = '1938334261.17304285801730428579'
    -- and page_location like '/company_page/copyleaks'

    -- and session_id = '81c6b6c33cf42b4c246ade9b26c9d823ga_session_id'
    -- and event__string_value like '%/company_page/demostack%'
    -- and event_id in ('78b947a1c575bccbd8c3c0c93439600e', '4f758068a241962004aa9bae3f7e1562', '7877c579ecbeb2eb6817844c15ce8d97')
)
-- select * from source limit 100 /*

, pivoted as (
    select
        * except (event_key, event__string_value, engagement_time_seconds),

        sum(engagement_time_seconds) as engagement_time_seconds,

        {{ max_by_event_key(event_keys) }}

    from source

    group by all
)
-- select * from pivoted order by event_datetime_utc /*
-- select pivoted.*
-- from pivoted
-- left join {{ ref('sessions') }} on pivoted.session_id = sessions.session_id
-- where user_type = 'Startup' 
-- order by user_pseudo_id /*
-- select *
-- from pivoted
-- where event_date = '2024-11-18'
-- and page_location like '%/company_page/copyleaks%'
-- -- and (filled_user_id or filled_user_pseudo_id)
-- order by user_pseudo_id, session_id, event_datetime_utc /*

, with_url_components as (
    select
        *,

        {{ url_page_path_and_query() }}

    from pivoted
)

, with_grouped_page as (
    select
        *,

        {{ grouped_page_path_and_name(column='referrer_path') }}

        {{ grouped_page_path_and_name(column='page_path') }}

    from with_url_components
)
-- select * from with_grouped_page /*

, fixed_values as (
    select
        * except (tab, section, entity_type,page_title),

        initcap(trim(
            replace(
                replace(
                    case
                        when
                            (event_name != 'profile_interactions' or lower(entity_type) like '%tabs')
                            and grouped_page_path in ('/company_page', '/investor_page')
                            and tab is null
                                then 'overview'

                        else tab
                    end,
                    '-',
                    ' '
                ),
                '_',
                ' '
            )
        )) as tab,

        initcap(case
            when section = 'Description Cleantech More'
                then 'Description Read More'

            else section
        end) as section,

        initcap(case
            when entity_type = 'Moltinational Tabs'
                then 'Multinational Tabs'

            else entity_type
        end) as entity_type,
        case when page_title like '%|%'
            then trim(split(page_title, '|')[offset(0)])
            else page_title
        end as page_title,

    from with_grouped_page
)
-- select grouped_page_name, tab, count(*) c from fixed_values group by all order by c desc /*
-- select * from fixed_values where grouped_page_name is null or grouped_page_name = '' /*
-- select * from fixed_values where session_id = '366408232.17307199071730719906' order by event_datetime_utc /*

, with_previous_page as (
    select
        *,

        if(
            session_id is not null,
            lag(page_path) over (
                partition by session_id
                order by event_datetime_utc, batch_sort_key
            ),
            null
        ) as previous_page_path,

    from fixed_values
)
-- select * from with_previous_page order by session_id, event_datetime_utc /*

, with_page_view_rank as (
    select
        *,

        case
            when event_name in ('session_start', 'first_visit', 'first_open')
                then 1

            when session_id is not null
                then countif(
                    -- (event_name = 'page_view' and previous_page_path is not null) or
                    (page_path is not null and previous_page_path is null)
                    or (page_path != previous_page_path and page_path is not null and previous_page_path is not null)
                ) over (
                    partition by session_id
                    order by event_datetime_utc, batch_sort_key
                    rows between unbounded preceding and current row
                )
        end as page_view_rank,

    from with_previous_page
)
-- select session_id, event_datetime_utc, batch_sort_key, event_name, page_view_rank, page_location
-- from with_page_view_rank
-- where session_id != '-1-1'
-- and session_id = '100004989.17271765941727176593'
-- order by session_id, event_datetime_utc, batch_sort_key /*

-- , with_user_id as (
--     select
--         coalesce(
--             if(
--                 session_id != '{{ var("empty_session_id") }}',

--                 last_value(nullif(user_id, '{{ var("empty_user_id") }}') ignore nulls)
--                     over(partition by session_id order by page_view_rank desc),
                    
--                 null
--             ),
--             '{{ var("empty_user_id") }}'
--         ) as user_id,

--         * except (user_id),

--     from pivoted
-- )
-- -- select * from with_user_id order by session_id, page_view_rank, batch_sort_key /*

, with_engagement_time as (
    select 
        *,

        case
            when session_id is not null and session_id != '{{ var("empty_session_id") }}'
            then lead(engagement_time_seconds) over(
                partition by session_id, page_view_rank
                order by session_id, page_view_rank, batch_sort_key
            )
        end as engagement_time_seconds_lead,

        case
            when session_id is not null and session_id != '{{ var("empty_session_id") }}'
            then sum(engagement_time_seconds) over (
                partition by session_id, page_view_rank
            )
        end as page_view_engagement_time_seconds,

        -- case
        -- when session_id != '{{ var("empty_session_id") }}'
        --     then cast(round(
        --         sum(engagement_time_msec) over (
        --             partition by session_id, event_id
        --             order by event_datetime_utc, batch_sort_key
        --             range between unbounded preceding and current row
        --         )
        --     ) / 1000 as int)
        -- end as page_view_engagement_time_seconds_accumulated,

    from with_page_view_rank
)
select * from with_engagement_time
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY user_id DESC) = 1


-- select event_id, count(*) c from with_engagement_time group by 1 having c > 1 order by c desc /*
-- select * from with_engagement_time where event_id = 'a02aa8c1ad25d2ed4c43b92cf5d0ceaa' /*

-- select * from with_engagement_time where event_name = 'public_watchlists' /*
-- where page_location like '%/company_page/2bprecise%' and event_date = '2024-11-18'
-- where page_location like '%/company_page/armis%'
-- where session_id = '2088743292.17264543561730458643'
-- order by session_id, page_view_rank, batch_sort_key /*
/**/