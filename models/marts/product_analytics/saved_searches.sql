{{ config(
    materialized='table',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "saved_search_created_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['user_id', 'saved_search_id', 'saved_search_type'],
    tags=['product_analytics']
) }}

with source as (
    select *

    from {{ ref('int_saved_searches') }}
)

-- {#
-- , users as (
--     select
--         user_id,

--         {{ user_dimensions() }}

--     from {{ ref('sessions') }}

--     where user_id is not null

--     qualify row_number() over (
--         partition by user_id
--         order by is_user_registered desc, user_signup_date, user_signup_start_datetime
--     ) = 1
-- )
-- #}

, ga4_dimensions as (
    select
        user_id,

        {{ user_dimensions() }}

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions() }}

    from {{ ref('sessions') }}

  where user_id is not null

  qualify row_number() over(
    partition by user_id
    order by session_date desc
  ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key([
        'source.user_id',
        'saved_search_id',
    ]) }} as record_id,

    source.*,

    ga4_dimensions.* except (user_id)

from source
left join ga4_dimensions
    on source.user_id = ga4_dimensions.user_id