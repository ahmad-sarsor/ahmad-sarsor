{{ config(
    materialized='incremental',
    schema=var('intermediate_schema'),
    unique_key='record_id',
    partition_by={
        "field": "date_day",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['session_id', 'user_pseudo_id', 'page_path', 'grouped_page_path'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}
{%- set filters = {
    'semantic': 'semantic',

    'page': 'page',
    'status': 'status',
    'sort': 'sort',

    'searchname': 'search_name',
    'alltags': 'all_tags',
    'anytag': 'any_tag',
    'excludetags': 'exclude_tags',

    'sectorclassification': 'sector_classification',
    'primarysectorclassification': 'primary_sector_classification',
    'targetcustomer': 'target_customer',
    'coretechnology' :'core_technology',
    'fundingstages': 'funding_stages',
    'lowerFoundedYear': 'founded_year_min',
    'upperFoundedYear': 'founded_year_max',
    'totalraisedmin': 'total_raised_min',
    'totalraisedmax': 'total_raised_max',
    'marketcapmin': 'market_cap_min',
    'marketcapmax': 'market_cap_max',
    'businessmodels': 'business_models',
    'productstage': 'product_stage',
    'employees': 'employees',
    'location': 'location',

    'fundingtype': 'funding_type',
    'investmentstage': 'investment_stage',
    'investmentrangemin': 'investment_range_min',
    'investmentrangemax': 'investment_range_max',
    'managedassetsmin': 'managed_assets_min',
    'managedassetsmax': 'managed_assets_max',
    'portfoliosizemin': 'portfolio_size_min',
    'portfoliosizemax': 'portfolio_size_max',
    'nationality': 'nationality',

    'hubtype': 'hub_type',
    'batchsize': 'batch_size',

    'officetype': 'office_type',

    'acquisitionsmin': 'acquisitions_min',
    'acquisitionsmax': 'acquisitions_max',
    'employeesworldwide': 'employees_worldwide',
    'otheractivities': 'other_activities',
    'forbesrankmin': 'forbes_rank_min',
    'forbesrankmax': 'forbes_rank_max',
 } -%}
{#{%- set events = [
    'page_view',
    'advanced_search_filters',
    'search_page_tabs',
    'profile_visit_after_search',
    'page_analytics_section',
] -%}#}
with source as (
    select
        user_id,
        user_pseudo_id,
        session_id,
        session_date,

        event_id,
        event_date,
        event_datetime_utc,
        batch_sort_key,
        -- event_name,

        page_referrer,
        case when page_referrer like '%startupnationcentral%' then grouped_referrer_path end as grouped_referrer_path,

        page_referrer is null or (page_referrer like '%startupnationcentral%' and grouped_referrer_path = '/') as is_from_main_page,
        (page_referrer is null or page_referrer not like '%startupnationcentral%') as is_from_referrer,

        page_title,
        page_location,
        page_query,

        page_path,
        grouped_page_path,
        
        company_url_name,
        
        initcap(
            replace(
                case 
                    when event_name = 'search_page_tabs'
                        then element_clicked

                    when tab = 'Overview'
                        then null

                    else tab
                end,
                '_', ' '
            )
        ) as tab,

        ends_with(page_path, '/search') as is_search,

        case
            when ends_with(page_path, '/search')
                then split(grouped_page_path, '/')[safe_offset(1)]
        end as profile,
        
        page_location like '%semantic=%' as is_semantic_search,
        regexp_extract(page_query, r"[&\?]?semantic=([^\?&=]+)") as semantic_search_id,

        lag(page_title) over (partition by session_id order by event_datetime_utc, batch_sort_key) as previous_page_title,
        lag(page_location) over (partition by session_id order by event_datetime_utc, batch_sort_key) as previous_page_location,

    from {{ ref('stg_ga4__events_pivoted') }}

    {{ incremental_predicate(
        history_days,
        source_column='event_date',
        target_column='date_day',
        where_=True
    ) }}


    -- where event_date = '2025-03-21'
    -- and session_id = '513895127.17413585591742544831'

    -- where event_date = '2025-03-20'
    -- and user_pseudo_id = '973607840.1737291055'
    -- and session_id = '973607840.17372910551742475585'

    -- where event_date = '2025-03-13'
    -- and user_pseudo_id = '1309366378.1741895644'

    -- where event_date = '2025-01-20'
    -- and user_id = 'M3UMgSfOERXuaj2ekD18I8quRFoDPtoI9m80FLM4rMf5SrCjKBiMXY'

    -- and (event_date = '2025-02-22' or event_date = '2025-02-23')
    -- and (user_pseudo_id = '2116113247.1733803276' or user_pseudo_id = '1370757871.1740182160')
    -- and event_date >= '2024-08-01'
    -- and user_pseudo_id = '328390338.1722631205'
    -- and event_date >= '2025-02-01'
    -- and session_id in (
        -- '268022749.17183072751722935231',
        -- '1198025033.17211407941722926375',
        -- '1805628941.17227775131722777513',

        -- '1495354819.17231943331723197613',
        -- '1533006280.17234843531723542782',
        -- '1253877067.17244515481724451548',
        -- '2012629669.17235137561723614005',
        -- '1932176412.17251031051725103105'

        -- '1002670699.17104829551723199910'

        -- '1008799911.17241925331724192532' -- utm
        -- '1641354817.17366644001736664399'
    -- )

    -- and session_id = '399865970.17251928051725192805'
    -- and session_id = '366408232.17307199071730719906' -- multiple user ids
    -- and session_id = '1861334396.17393041771739304177'
    -- and session_id = '301603051.17078522241738916847'
    -- and session_id = '1725604992.17394952541739495253'
)
-- select * from source order by session_id, event_datetime_utc /*
-- select session_id, count(*), count(distinct grouped_page_path) from source group by all order by 3 desc, 2 desc /*
-- select grouped_page_path, count(*) from source group by all order by 1 /*
-- select * from source where is_search and page_query is null order by session_id, event_datetime_utc /*
-- select session_id, count(*) c from source where page_location like '%sort=%page=%' group by 1 order by c desc /*
-- select * from source where session_id = '399865970.17251928051725192805' order by event_datetime_utc /*
-- select * from source where session_id = '2103534733.17230980391725968642' order by event_datetime_utc /*
-- select * from source where session_id = '366408232.17307199071730719906' order by event_datetime_utc /*

, source_deduped as (
    select *

    from source

    where (
        -- is_search != true or
        previous_page_location is null
        or page_location != previous_page_location
        or page_title != previous_page_title
    )
)
-- select * from source_deduped order by session_id, event_datetime_utc, batch_sort_key /*

, semantic_mysql as (
    select *

    from {{ ref('stg_mysql__semantic_search') }}

    -- where user_id = 'M3UMgSfOERXuaj2ekD18I8quRFoDPtoI9m80FLM4rMf5SrCjKBiMXY'
)
-- select * from semantic_mysql /*

, sessions as (
    select
        session_id,
        session_date,
        session_duration,
        user_id,
        suspected_fraud,

        {{ user_dimensions() }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions() }}

    from {{ ref('sessions') }}
)
-- select * from sessions where session_id = '2103534733.17230980391725968642' /*

, classification as (
    select
        classification_id,
        root_key,
        sector_name as classification_name,
        url_name as classification_url_name,
        -- classification_type,

    from {{ ref('stg_mysql__base_classification_model') }}
)

, with_lag as (
    select
        * except (user_id),

        lag(is_search) over (
            partition by session_id
            order by event_datetime_utc, batch_sort_key
        ) as is_previous_event_search,

        date_diff(
            event_datetime_utc,
            lag(event_datetime_utc) over (
                partition by session_id
                order by event_datetime_utc, batch_sort_key
            ),
            second
        ) as time_from_previous_event,

        case when is_search then date_diff(
            event_datetime_utc,
            lag(event_datetime_utc) over (
                partition by session_id, is_search
                order by event_datetime_utc, batch_sort_key
            ),
            second
        ) end as time_from_previous_search,

        case when is_search then lag(grouped_page_path) over (
            partition by session_id, is_search
            order by event_datetime_utc, batch_sort_key
        ) end as previous_search_path,

        case when is_search then lag(page_title) over (
            partition by session_id, is_search
            order by event_datetime_utc, batch_sort_key
        ) end as previous_search_title,

        case when is_search then lag(is_semantic_search) over (
            partition by session_id, is_search
            order by event_datetime_utc, batch_sort_key
        ) end as previous_search_is_semantic,

        case when is_search then lag(semantic_search_id) over (
            partition by session_id, is_search
            order by event_datetime_utc, batch_sort_key
        ) end as previous_semantic_search_id,

    from source_deduped
)
-- select * from with_lag order by event_datetime_utc /*
-- select * from with_lag where is_search is true order by event_datetime_utc /*
-- select * from with_lag where session_id = '366408232.17307199071730719906' order by event_datetime_utc /*

, with_last_semantic as (
    select
        * except (previous_semantic_search_id),

        case when is_semantic_search then
            last_value(previous_semantic_search_id ignore nulls) over (
                partition by session_id, is_semantic_search
                order by event_datetime_utc, batch_sort_key
                rows between unbounded preceding and current row
            )
        end as previous_semantic_search_id,

    from with_lag
)
-- select session_id, page_query, is_semantic_search, semantic_search_id, previous_semantic_search_id from with_last_semantic order by event_datetime_utc, batch_sort_key /*

, with_rank as (
    select
        *,

        countif(
            is_search and (
                (is_semantic_search != previous_search_is_semantic)
                or (semantic_search_id != previous_semantic_search_id)
                or not is_semantic_search and (
                    time_from_previous_search is null
                    or grouped_page_path != previous_search_path
                    or (page_title != previous_search_title and page_location != previous_page_location)
                    or time_from_previous_search >= 180 -- 3 minute
                    or (not is_previous_event_search and time_from_previous_event >= 180) -- 1 minute
                )
            )
            -- or (is_semantic_search and page_title != previous_page_title)
        ) over (
            partition by session_id
            order by event_datetime_utc, batch_sort_key
            range between unbounded preceding and current row
        ) as search_rank,

    from with_last_semantic
)
-- select * from with_rank order by event_datetime_utc /*

, with_search_id as (
    select
        -- *,

        * except (
            previous_page_location,
            previous_page_title,
            is_previous_event_search,
            time_from_previous_event,
            time_from_previous_search,
            previous_search_path,
            previous_search_title,
            previous_search_is_semantic,
            search_rank
        ),

        concat(
            session_id,
            '_',
            session_date,
            '_',
            cast(search_rank as string)
        ) as search_id,

        -- time_from_previous_search is null as is_first_search,
        -- grouped_page_path != previous_search_path as is_path_changed,
        -- page_title != previous_search_title as is_title_changed,
        -- is_semantic_search != previous_search_is_semantic as is_semantic_search,
        -- semantic_search_id != previous_semantic_search_id as is_new_semantic_id,
        -- time_from_previous_search >= 180 as threshold_time_passed_from_previous_search, -- 3 minute
        -- not is_previous_event_search and time_from_previous_event >= 180 as threshold_time_passed_from_previous_event, -- 3 minute

    from with_rank
)
-- select * from with_search_id order by session_id, event_datetime_utc, batch_sort_key /*
-- select * from with_search_id where event_id = '0010fa5db20a2c181eaca27d9856b402' /*
-- select * from with_search_id where session_id = '1309366378.17418956441741895643' /*
-- select * from with_search_id where search_id = '1309366378.17418956441741895643_2025-03-13_3' /*

-- this is why we use time > 3 minutes for search rank sensitivity
-- select event_datetime_utc, page_title, page_query, search_rank, is_search,
-- round(time_from_previous_event / 60, 2) time_from_previous_event,
-- round(time_from_previous_search / 60, 2) time_from_previous_search,
-- from with_search_id 
-- where event_date = '2025-03-13'
-- and user_pseudo_id = '1309366378.1741895644'
-- order by session_id, event_datetime_utc /*

, filter_search_ga4 as (
    select *
    
    from with_search_id

    where not is_semantic_search
)
-- select * from filter_search_ga4 order by session_id, event_datetime_utc /*

, semantic_search_ga4 as (
    select *
    
    from with_search_id

    -- where is_semantic_search
    where semantic_search_id is not null
)
-- select * from semantic_search_ga4 order by session_id, event_datetime_utc /*

, semantic_search as (
    select
        semantic_search_ga4.*,
        
        semantic_mysql.user_input as semantic_user_input,
        semantic_mysql.ai_analysis as semantic_ai_analysis,

        p.filter_name,
        p.filter_value,

        if(length(p.filter_value) > 20, p.filter_value, null) as classification_id,

    from semantic_search_ga4

    left join semantic_mysql
        on semantic_search_ga4.semantic_search_id = semantic_mysql.id

    cross join unnest(`finderv2`.`Intermediate`.json_pairs(semantic_mysql.filters)) as p
    where p.filter_name not in ('explain', 'prompt_id', 'type')
)
-- select * from semantic_search order by session_id, event_datetime_utc /*

, ranked_search as (
    select *

    from filter_search_ga4

    where is_search
)
-- select * from ranked_search order by session_id, event_datetime_utc /*

, query_params as (
    select
        -- * except (qp),
        event_id,

        split(qp, '=')[safe_offset(0)] as param_name,

        trim(
            replace(
                replace(
                    replace(
                        replace(
                            replace(
                                split(qp, '=')[safe_offset(1)],
                                '%2B', '|'
                            ), -- %2B is '+', but later on we split on '|'
                            '%7C', '|'
                        ),
                        '%20', ' '
                    ),
                    '+', ' '
                ),
                '%2C', ','
            ),
        '|') as param_value,

    from ranked_search

    left join unnest(split(page_query, '&')) as qp
)
-- select * from query_params order by event_id /*

, filtered_params as (
    select *

    from query_params

    where param_name is null or param_name in {{ array_to_sql_list(filters.keys()) }}
)
-- select * from filtered_params order by event_id /*

, split_values as (
    select 
        -- filtered_params.* except (param_value),
        event_id,
        param_name as filter_name,
        value as filter_value,

        if(length(value) > 20, value, null) as classification_id,

    from filtered_params
    
    left join unnest(split(param_value, '|')) as value

    qualify row_number() over (partition by event_id, filter_name, filter_value) = 1
)
-- select * from split_values order by event_id /*

, with_params as (
    select *

    from filter_search_ga4

    left join split_values using (event_id)
)
-- select * from with_params order by session_id, event_datetime_utc /*
-- select event_id, session_id, event_datetime_utc, is_from_main_page, is_from_referrer, page_title, page_location, page_path, grouped_page_path, page_query, is_search, filter_name, filter_value, 
-- from with_params order by session_id, event_datetime_utc /*

, unioned as (
    select * from semantic_search

    union all by name

    select
        with_params.*,

        cast(null as string) as semantic_user_input,
        cast(null as string) as semantic_ai_analysis,
    
    from with_params
)
-- select * from unioned order by user_pseudo_id, event_datetime_utc /*

, with_classification as (
    select
        unioned.*,
        classification.classification_name,
        nullif(classification.root_key, classification.classification_id) as root_key,

    from unioned

    left join classification using (classification_id)
)

, with_root_classification as (
    select
        with_classification.* except (classification_id, root_key),
        root_classification.classification_name as classification_parent_name,

    from with_classification

    left join classification as root_classification
        on with_classification.root_key = root_classification.classification_id
)
-- select * from with_root_classification where session_id = '2103534733.17230980391725968642' /*
-- select param_name, if(param_name in {{ array_to_sql_list(filters) }}, 1, 2), count(*) c
-- from with_root_classification
-- group by all
-- order by 2, 3 desc

, profiles_viewed_cte as (
    select
        search_id,

        string_agg(company_url_name order by min_ts) as profiles_viewed,
        array_agg(company_url_name order by min_ts) as profiles_arr,
        
        count(distinct company_url_name) as profiles_unique_count,
        sum(c) as profiles_total_count,

    from (
        select
            search_id,
            company_url_name,
            min(event_datetime_utc) as min_ts,
            count(*) as c,

        from unioned

        where company_url_name is not null

        group by 1, 2
    ) t

    group by 1
)
-- select * from profiles_viewed_cte limit 10 /*

, tabs_cte as (
    select
        search_id,

        string_agg(tab order by min_ts) as tabs_clicked,
        array_agg(tab order by min_ts) as tabs_arr,
        
        count(distinct tab) as tabs_unique_count,
        sum(c) as tabs_total_count,

    from (
        select
            search_id,
            tab,
            min(event_datetime_utc) as min_ts,
            count(*) as c,

        from unioned

        where tab is not null

        group by 1, 2
    ) t

    group by 1
)
-- select * from tabs_cte limit 10 /*

, merged as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'root_classification.event_date',
            'root_classification.session_id',
            'root_classification.event_id',
            'root_classification.filter_name',
            'root_classification.filter_value',
        ]) }} as record_id,

        coalesce(sessions.session_date, root_classification.event_date) as date_day,

        if(left(user_id, 1) = '_', null, user_id) as user_id,

        sessions.* except (session_id, session_date, user_id),

        root_classification.* except (event_date),

        profiles_viewed_cte.* except (search_id),

        tabs_cte.* except (search_id),

    from with_root_classification as root_classification

    left join profiles_viewed_cte
        on root_classification.search_id = profiles_viewed_cte.search_id

    left join tabs_cte
        on root_classification.search_id = tabs_cte.search_id

    left join sessions
        on root_classification.session_id = sessions.session_id
        and root_classification.session_date = sessions.session_date
)
-- select * from merged order by event_datetime_utc /*

, renamed_filters as (
    select
        * except (
            is_semantic_search,
            semantic_search_id,
            semantic_user_input,
            semantic_ai_analysis,
            profile,
            company_url_name,
            tab,
            tabs_clicked,
            tabs_arr,
            tabs_unique_count,
            tabs_total_count,
            profiles_viewed,
            profiles_arr,
            profiles_unique_count,
            profiles_total_count,
            filter_name,
            filter_value,
            classification_name,
            classification_parent_name
        ),

        initcap(case
            when profile = 'startups'
                then 'company'

            when right(profile, 1) = 's'
                then left(profile, length(profile) - 1)

            else profile
        end) as profile,

        is_semantic_search,
        semantic_search_id,
        semantic_user_input,
        semantic_ai_analysis,

        tabs_clicked,
        tabs_arr,
        tabs_unique_count,
        tabs_total_count,

        profiles_viewed,
        profiles_arr,
        profiles_unique_count,
        profiles_total_count,

        case
            {% for source_name, target_name in filters.items() -%}
                when filter_name = '{{ source_name }}' then '{{ target_name }}'
            {% endfor %}

                else filter_name
        end as filter_name,

        filter_value,
        classification_name,
        classification_parent_name,

    from merged
)

, with_filter_category as (
    select
        * except (
            filter_name,
            filter_value,
            classification_name,
            classification_parent_name
        ),

        initcap(replace(case
            {% for target_name in filters.values() -%}
                when filter_name = '{{ target_name }}'
                then if(
                    right(filter_name, 4) in ('_min', '_max'),
                    left('{{ target_name }}', length('{{ target_name }}') - 4),
                    '{{ target_name }}'
                )
            {% endfor %}

            else filter_name
        end, '_', ' ')) as filter,
        
        filter_name,
        filter_value,

        classification_name,
        classification_parent_name,

    from renamed_filters
)

select * except (is_search)

from with_filter_category 

where
    is_search is true
    and not is_from_main_page
    and grouped_page_path like '%/search'
    
/*


select
    record_id, event_id, user_pseudo_id, session_id, session_date, event_datetime_utc, page_location, page_title, page_query, page_path, grouped_page_path,
    search_id, filter_name, filter_value, classification_name, classification_parent_name, profiles_viewed, profiles_viewed_count, profiles_viewed_unique_count,
from renamed_filters
order by session_id, event_datetime_utc

/**/