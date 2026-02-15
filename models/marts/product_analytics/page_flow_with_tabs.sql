{{ config(
    materialized='incremental',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "session_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['user_id', 'session_id', 'grouped_page_path_1', 'grouped_page_name_1'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with source as (
    select *

    from {{ ref('bi_page_flow_with_tabs') }}

    {{ incremental_predicate(
        history_days,
        source_column='session_date',
        target_column='session_date',
        where_=True
    ) }}

    {# where session_date = '2025-02-01' and user_pseudo_id = '791581970.1738386466' #}
)
{# select * from source /* #}
{# select event_datetime_utc, session_id, page_title, grouped_page_name, previous_grouped_page_name from source order by event_datetime_utc /* #}

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
{# select session_id, session_date from sessions where session_id = '1142668579.17337852521734825255' /* #}

select *

from source

left join sessions using(session_id, session_date)

/**/