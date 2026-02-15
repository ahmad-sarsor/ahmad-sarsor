{{ config(
    materialized='incremental',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "date_day",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['session_id', 'user_pseudo_id', 'user_id', 'search_id'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}
{%- set categorical_columns = [
    'sort',
    'search_name',
    'all_tags',
    'any_tag',
    'exclude_tags',
    'funding_stage',
    'business_models',
    'product_stage',
    'employees',
    'funding_type',
    'investment_stage',
    'nationality',
    'hub_type',
    'batch_size',
    'office_type',
    'employees_worldwide',
] %}
{%- set classification_columns = [
    'sector_classification',
    'primary_sector_classification',
    'target_customer',
    'core_technology',
] %}
{%- set range_columns = [
    'founded_year',
    'total_raised',
    'market_cap',
    'investment_range',
    'managed_assets',
    'portfolio_size',
    'acquisitions',
    'forbes_rank',
] -%}

with source as (
    select
        date_day,
        user_id,
        user_pseudo_id,
        session_id,
        event_datetime_utc,
        event_id,
        search_id,
        is_semantic_search,
        semantic_search_id,
        semantic_user_input,
        semantic_ai_analysis,
        profile,
        filter_name,
        filter_value,
        classification_name,
        classification_parent_name,
        tabs_clicked,
        tabs_arr,
        tabs_unique_count,
        tabs_total_count,
        profiles_viewed,
        profiles_arr,
        profiles_unique_count,
        profiles_total_count,

        case
            when filter_name = 'page' then safe_cast(filter_value as int)
        end as page_index,

    from {{ ref('int_search_filters') }}

    where filter_name is not null

    {{ incremental_predicate(
        history_days,
        source_column='date_day',
        target_column='date_day',
        and_=True
    ) }}

    -- and date_day = '2025-03-13'
    -- and user_pseudo_id = '1309366378.1741895644'

    -- and session_id = '301603051.17078522241738916847'
    -- and session_id = '1725604992.17394952541739495253'
    -- and session_id = '1836509132.17390270781739027078'
)
-- select * from source order by date_day, event_datetime_utc /*

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

, event_agg as (
    select
        search_id,
        event_id,

        {% for column in range_columns -%}
            concat(
                min(case when filter_name like '{{ column }}_%' then filter_value end),
                '-',
                max(case when filter_name like '{{ column }}_%' then filter_value end)
            ) as {{ column }}_range,
        {% endfor %}

    from source
    
    group by all
)
-- select * from event_agg order by search_id /*

, search_pages_agg as (
    select
        search_id,
        
        array_agg(
            distinct page_index
            ignore nulls
            order by page_index
        ) as search_pages,

        count(distinct page_index) as search_page_count,

    from source

    group by 1
)

, agg as (
    select
        date_day,
        user_id,
        user_pseudo_id,
        session_id,

        source.search_id,
        min(event_datetime_utc) as search_started_datetime,
        profile,

        is_semantic_search,
        semantic_search_id,
        semantic_user_input,
        semantic_ai_analysis,

        max_by(profiles_viewed, event_datetime_utc) as profiles_viewed,
        max_by(profiles_arr, event_datetime_utc) as profiles_arr,
        max(profiles_unique_count) as profiles_unique_count,
        max(profiles_total_count) as profiles_total_count,

        max_by(tabs_clicked, event_datetime_utc) as tabs_clicked,
        max_by(tabs_arr, event_datetime_utc) as tabs_arr,
        max(tabs_unique_count) as tabs_unique_count,
        max(tabs_total_count) as tabs_total_count,

        nullif(array_to_string(array(select cast(x as string) from unnest(search_pages_agg.search_pages) as x), ','), '') as search_pages,
        max(search_pages_agg.search_page_count) as search_page_count,

        {% for column in categorical_columns %}
            string_agg(distinct case
                when filter_name = '{{ column }}'
                    then filter_value
            end) as {{ column }},

            count(distinct case
                when filter_name = '{{ column }}'
                    then filter_value
            end) as {{ column }}_count,
        {% endfor %}


        {% for column in classification_columns %}
            string_agg(distinct case
                when filter_name = '{{ column }}'
                    then classification_name
            end) as {{ column }},

            count(distinct case
                when filter_name = '{{ column }}'
                    then classification_name
            end) as {{ column }}_count,

            string_agg(distinct case
                when filter_name = '{{ column }}'
                    then classification_parent_name
            end) as {{ column }}_parent,

            count(distinct case
                when filter_name = '{{ column }}'
                    then classification_parent_name
            end) as {{ column }}_parent_count,
        {% endfor %}


        {% for column in quantitative_columns %}
            min(case
                when filter_name = '{{ column }}_min'
                    then filter_value
            end) as {{ column }}_min,

            max(case
                when filter_name = '{{ column }}_max'
                    then filter_value
            end) as {{ column }}_max,

            string_agg(distinct case
                when filter_name like '{{ column }}_%'
                    then {{ column }}_range
            end) as {{ column }}_ranges,

            count(distinct case
                when filter_name like '{{ column }}_%'
                    then {{ column }}_range
            end) as {{ column }}_ranges_count,
        {% endfor %}

    from source

    left join search_pages_agg
        on source.search_id = search_pages_agg.search_id

    left join event_agg
        on source.search_id = event_agg.search_id
        and source.event_id = event_agg.event_id

    group by all
)

, merged as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'agg.date_day',
            'agg.search_id',
        ]) }} as record_id,

        coalesce(sessions.session_date, agg.date_day) as date_day,

        sessions.* except (session_id, session_date, user_id),

        agg.* except (date_day),

    from agg

    left join sessions
        on agg.session_id = sessions.session_id
        and agg.date_day = sessions.session_date
)
select * from merged /*

-- select record_id, count(*) c from merged group by 1 having c > 1 order by c desc /*
-- select * from merged where record_id = '6e392ee977b84c6aeef60b12ab76ff7f' /*
-- order by search_id /*
-- order by user_pseudo_id, date_day /*

/**/