{{ config(
    enabled=false,
    materialized='incremental',
    schema=var('ga4_staging_schema'),
    unique_key='audience_membership_id',
    partition_by={
        "field": "occurrence_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['pseudo_user_id']
) }}

{% set partitions_to_replace = 3 %}

with source as (
    select *
    from {{ source('ga4', 'pseudonymous_users_*') }}
    where
        {% if is_incremental() %}
            _table_suffix between
            FORMAT_DATE(
                '%Y%m%d',
                DATE_SUB(
                    CURRENT_DATE(), interval {{ partitions_to_replace }} day
                )
            )
            and FORMAT_DATE('%Y%m%d', CURRENT_DATE())
            and DATE((PARSE_DATE('%Y%m%d', _table_suffix))) > (
                select DATE(MAX(occurrence_date))
                from {{ this }}
            )
        {% else %}
        -- On the first run, process all available data
        _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'occurrence_date',
        'pseudo_user_id',
        'ad.id'
    ]) }} as audience_membership_id,
    DATE(PARSE_DATE('%Y%m%d', occurrence_date)) as occurrence_date,
    pseudo_user_id,
    ad.id as audience__id,
    ad.name as audience__name,
    case
        when
            ad.membership_start_timestamp_micros
            < ad.membership_expiry_timestamp_micros
            then
                DATETIME(
                    TIMESTAMP_MICROS(ad.membership_start_timestamp_micros),
                    'UTC'
                )
        else
            DATETIME(
                TIMESTAMP_MICROS(ad.membership_expiry_timestamp_micros), 'UTC'
            )
    end as audience__membership_start,
    case
        when
            ad.membership_expiry_timestamp_micros
            > ad.membership_start_timestamp_micros
            then
                DATETIME(
                    TIMESTAMP_MICROS(ad.membership_expiry_timestamp_micros),
                    'UTC'
                )
        else
            DATETIME(
                TIMESTAMP_MICROS(ad.membership_start_timestamp_micros), 'UTC'
            )
    end as audience__membership_expiry,
    ad.npa as audience__npa,
    PARSE_DATE('%Y%m%d', last_updated_date) as last_updated_date
from source,
    UNNEST(audiences) as ad
