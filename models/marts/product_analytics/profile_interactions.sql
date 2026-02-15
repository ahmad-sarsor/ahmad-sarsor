{{ config(
    materialized='incremental',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "date_day",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['date_day', 'user_pseudo_id', 'session_id', 'company_url_name'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

with source as (
    select *

    from {{ ref('bi_profile_interactions') }}

    {{ incremental_predicate(
        history_days,
        source_column='date_day',
        target_column='date_day',
        where_=True
    ) }}
)

, sessions as (
    select
        session_id,
        session_date,
        session_duration,

        {{ user_dimensions() }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions() }}

    from {{ ref('sessions') }}
)

, companies as (
    select
        {{ company_dimensions(except=['company_url_name']) }}

    from {{ ref('int_companies') }}
)

, merged as (
    select *

    from source
    left join sessions using (session_id, session_date)
    left join companies using (company_id)
)
select * from merged /*

{# select record_id, count(*) c from merged group by 1 having c > 1 order by c desc /* #}
/**/