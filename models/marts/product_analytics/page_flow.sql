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

    from {{ ref('bi_page_flow') }}

    {{ incremental_predicate(
        history_days,
        source_column='session_date',
        target_column='session_date',
        where_=True
    ) }}
)
{# select * from source /* #}

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

left join sessions using (session_id, session_date)

/**/