{{ config(
    materialized='incremental',
    schema=var('intermediate_schema'),
    unique_key='record_id',
    partition_by={
        "field": "session_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['user_id', 'session_id', 'grouped_page_path', 'grouped_page_name'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}
{%- set event_names = [
    'page_view', 
    'profile_interactions'
] -%}

with source as (
    select
        {{ ga4_user_ids() }}

        {{ ga4_event_ids() }}

        page_location,
        
        case
            when event_name = 'page_view'
                then lag(page_location) over (
                    partition by session_id, event_name = 'page_view'
                    order by event_datetime_utc
                )
        end as previous_page_view_location,

        page_path,
        page_title,

        grouped_page_path,
        
        replace(grouped_page_name, ' Page', '') as grouped_page_name,
        
        tab,

    from {{ ref('stg_ga4__events_pivoted') }}

    where event_name in {{ array_to_sql_list(event_names) }}

    and (
        event_name != 'profile_interactions'
        or lower(entity_type) like '%tabs'
    )

    {{ incremental_predicate(
        history_days,
        source_column='event_date',
        target_column='session_date',
        and_=True
    ) }}
)
-- select * from source /*
-- select session_id, count(*) c from source group by all having c > 1 order by c desc /*
-- select * from source where session_id = '2110949844.17327110131733654051' and page_location = previous_page_view_location order by session_id, event_datetime_utc /*
-- select event_name, tab, count(*) c from source where event_name = 'profile_interactions' group by all order by 1, 2 /*
-- select * from source where event_name = 'profile_interactions' and tab is null limit 10 /*
-- select grouped_page_name, tab, count(*) c from source group by all order by c desc /*
-- select count(*) from source /*
-- select count(distinct session_id) from source /*

, filtered as (
    select *

    from source

    where (
        event_name != 'page_view'
        or previous_page_view_location is null
        or page_location != previous_page_view_location
    )
)
-- select count(*) from filtered /*
-- select * from filtered where event_name = 'page_view' and page_location = prev_page_location /*
-- select * from filtered where session_id = '2110949844.17327110131733654051' and page_location = previous_page_view_location order by session_id, event_datetime_utc /*

, with_rank as (
    select
        event_id as record_id,

        if(left(user_id, 1) = '_', null, user_id) as user_id,
        user_pseudo_id,
        session_id,
        session_date,
        
        event_name,
        event_datetime_utc,
        
        row_number() over (
            partition by session_id
            order by event_datetime_utc
        ) as session_event_rank,

        page_location,
        previous_page_view_location,
        page_path,
        page_title,

        grouped_page_path,
        grouped_page_name,
        tab,
        
        case
            when tab is not null -- and grouped_page_path in ('/company_page', '/investor_page')
                then coalesce(grouped_page_name, '') || ' - ' || tab
                
            else grouped_page_name
        end as grouped_page_name_with_tab,

    from filtered
)
select
    *,
    
    case
        when event_name = 'page_view'
            then lag(grouped_page_path) over (
                partition by session_id, event_name = 'page_view'
                order by event_datetime_utc
            )
    end as previous_grouped_page_path,
    
    case
        when event_name = 'page_view'
            then lag(grouped_page_name_with_tab) over (
                partition by session_id, event_name = 'page_view'
                order by event_datetime_utc
            )
    end as previous_grouped_page_name,

from with_rank /*

-- select count(*) from with_rank where is_user_registered is null /*
-- select user_id, user_pseudo_id, session_id, is_user_registered, session_date, event_datetime_utc
-- from with_rank
-- where session_id = '1142668579.17337852521734825255'
-- -- and is_user_registered is null
-- -- limit 10
-- qualify row_number() over (partition by is_user_registered) = 1

/**/