{{ config(
    materialized='incremental',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "date_day",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['session_id', 'page_title', 'page_path', 'page_location'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with source as (
    select *

    from {{ ref('page_views') }}

    where page_path like '/reports/%'
    and page_path != '/reports/reports and resources'

    {{ incremental_predicate(
        history_days,
        source_column='date_day',
        target_column='date_day',
        and_=True
    ) }}
)
-- select page_title, count(distinct page_path) c, string_agg(distinct rtrim(page_path, '/'), ', ') agg, rtrim(approx_top_count(page_path, 1)[safe_offset(0)].value, '/') as top_value
-- from source
-- where page_path like '/reports/%'
-- and page_path != '/reports/reports and resources'
-- group by all
-- having c > 1
-- order by c desc /*

, with_page_path_count as (
    select
        *,

        count(*) over (partition by page_title, page_path) as page_path_count,
        if(page_path like '%-%', 1, 0) as page_path_has_hyphen,

    from source
)
-- select distinct page_title, page_path, page_path_has_hyphen, page_path_count from with_page_path_count /*

, with_preferred_path as (
    select
        * except (page_path, page_path_has_hyphen, page_path_count),
        
        array_agg(rtrim(page_path, '/')) over (
            partition by page_title
            order by
                page_path_has_hyphen desc,
                page_path_count desc
        )[safe_offset(0)] as page_path,

    from with_page_path_count
)
-- select distinct page_title, page_path, page_path_has_hyphen, page_path_count, page_path_ from with_preferred_path /*

select *

from with_preferred_path

/**/