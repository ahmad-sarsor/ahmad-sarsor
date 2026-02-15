{{ config(
    materialized='table',
    schema=var('marts_schema'),
    tags=['product_analytics']
) }}

{%- set dimensions = [] -%}
{%- for item in var("user_dimensions") -%}
    {%- if not item.startswith('user_company_') and item not in ('user_signup_date', 'is_power_user') -%}
        {%- do dimensions.append(item) -%}
    {%- endif -%}
{%- endfor -%}

select
    user_id,

    date(coalesce(
        signup_complete_datetime,
        user_signup_date
    )) as user_signup_date,

    {{ user_dimensions(dimensions) }}
    user_business_email,

    entity_analysis_type,
    
    {{ ga4_event_dimensions() }}

    {{ marketing_dimensions() }}

    is_member,
    
    {{ company_dimensions(prefix='user_') }}

from {{ ref('int_users') }}

where not is_user_id_generated

qualify row_number() over (
    partition by user_id
    order by
        is_user_registered desc,
        is_user_confirmed desc,
        user_signup_date,
        signup_start_datetime
) = 1