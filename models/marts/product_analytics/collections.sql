{{ config(
    materialized='table',
    schema=var('marts_schema'),
    unique_key='record_id',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "list_created_date",
        "data_type": "date",
        "granularity": "month"
    },
    require_partition_filter=false,
    cluster_by=['owner_id', 'list_id', 'list_type'],
    tags=['product_analytics']
) }}

with source as (
    select *
    from {{ ref('int_collections') }}
    {% if is_incremental() %}
      where list_created_date >= date_sub(current_date(), interval 14 day)
    {% endif %}
),
ga4_dimensions as (
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
    {{ dbt_utils.generate_surrogate_key(['owner_id','list_id']) }} as record_id,
    source.*,
    ga4_dimensions.* except (user_id)
from source
left join ga4_dimensions
  on source.owner_id = ga4_dimensions.user_id
