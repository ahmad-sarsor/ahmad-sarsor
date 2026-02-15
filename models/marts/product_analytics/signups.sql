{{ config(
    materialized='table',
    schema=var('marts_schema'),
    unique_key='record_id',
    partition_by={
        "field": "signup_start_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['signup_start_date', 'user_id', 'signup_platform', 'signup_type'],
    tags=['product_analytics']
) }}

{%- set history_days = var("incremental_window_days") -%}

{%- set dimensions = [] -%}
{%- for item in var("user_dimensions") -%}
    {%- if not item.startswith('user_company_') and item not in ('is_power_user') -%}
        {%- do dimensions.append(item) -%}
    {%- endif -%}
{%- endfor -%}

{%- set users_dimensions_exclude = [
    'is_new_user',
    'is_returning_user',
    'suspected_fraud',
    'is_power_user',
] -%}


with signups as (
    select *

    from {{ ref('int_signups_pivoted') }}

    where left(user_id, 1) != '_'

    {#
    {{ incremental_predicate(
        history_days,
        source_column='signup_start_date',
        target_column='signup_start_date',
        and_=True
    ) }}
    #}

    {# and user_id = 'wcVMUGr5swFEq6PgwL7S66WkCE17IybpHa4nuNsloJoS0eGpHbY6ZW' #}
    
    -- where user_id = 'miJOBPN0BUYcOvbBGgyZZziaWaZPKh5JT2hs6sZ30ztYL37spWYaDF'
    -- and user_id in (
    --     "EGEbiV99GdyWyFP3bkCBOxApAjVkg6ocwtLgFokjNilx2CD0kBUGcD",
    --     "bj2BbDxlYOCfezEFIBhhGXH842apZo3jkeYE6BeXLrdQh72mw4B8a2",
    --     "t9phWp72CluVq3RUeb5iXIA5zpvX8mONYWqFYv2ZaI6dk9fhthobfb"
    -- )

    -- where signup_start_date = '2025-01-03' and user_id in (
    --     '6WvE4BcQzzFIAkem9g9jD4TDEogKYBIP7vJ5fty5YRr2QLrZ2YI4sq',
    --     'wnC8spTAGXyml1JVVuIIOXbKwNFzrG7gsVPkV9YKS3wBKPppltQl24'
    -- )
    
    -- and user_id = '1a9DZOViUe025SBPKNP4vdz5f8UcTFGZ9fn0mURfAfw9LURvXoDTS4'
    -- and signup_start_date = '2025-01-12' and user_id = '0Urie4dlNAqhCG22b6ERJl9QcEbZmHbzsEYnCTj3hfbHJHVvMYFaSJ'

    qualify row_number() over (partition by user_id order by signup_attempt desc) = 1
)
{# select * from signups order by signup_start_date /* #}

, sessions as (
    select
        user_id,
        session_id,

        coalesce(user_signup_start_datetime, user_signup_complete_datetime, user_signup_date) as _sort,

        {{ wrap_columns(dimensions) }}

    from {{ ref('sessions') }}

    where user_id is not null

    {# and user_id = 'wcVMUGr5swFEq6PgwL7S66WkCE17IybpHa4nuNsloJoS0eGpHbY6ZW' #}
    -- where session_id in (select max_session_id from signups)
    -- where user_id in (
    --     '6WvE4BcQzzFIAkem9g9jD4TDEogKYBIP7vJ5fty5YRr2QLrZ2YI4sq',
    --     'wnC8spTAGXyml1JVVuIIOXbKwNFzrG7gsVPkV9YKS3wBKPppltQl24'
    -- )
    -- and user_id = '1a9DZOViUe025SBPKNP4vdz5f8UcTFGZ9fn0mURfAfw9LURvXoDTS4'
    -- and user_id = '0Urie4dlNAqhCG22b6ERJl9QcEbZmHbzsEYnCTj3hfbHJHVvMYFaSJ'
)
{# select * from sessions /* #}

, users as (
    select
        user_id,
        session_id,

        {% for dim in dimensions %}
            {%- if dim in users_dimensions_exclude %}
                sessions.{{ dim }},
            {%- else %}
                if(sessions.user_id is not null, sessions.{{ dim }}, users.{{ dim }}) as {{ dim }},
            {%- endif -%}
        {%- endfor %}

    from (
        select *
        from sessions
        qualify row_number() over (partition by user_id order by _sort desc) = 1
    ) as sessions

    full outer join {{ ref("users") }} as users
        using (user_id)

    {# where user_id = 'wcVMUGr5swFEq6PgwL7S66WkCE17IybpHa4nuNsloJoS0eGpHbY6ZW' #}
    -- where user_id = '1a9DZOViUe025SBPKNP4vdz5f8UcTFGZ9fn0mURfAfw9LURvXoDTS4'
    -- where user_id = '0Urie4dlNAqhCG22b6ERJl9QcEbZmHbzsEYnCTj3hfbHJHVvMYFaSJ'
)
 --select * from users where user_id like 'tU5Ev9AQ6c8tYIZGPWbR0mqcy1Ug7Ga69fHc17lAwyTy2wSrfDqBVt'

, merged as (
    select
        coalesce(
            signups.record_id,
            {{ dbt_utils.generate_surrogate_key([
                'users.user_id'
            ]) }}
        ) as record_id,

        coalesce(signups.user_id, sessions.user_id, users.user_id) as user_id,
        coalesce(signups.max_session_id, sessions.session_id) as session_id,

        signups.* except (record_id, user_id, max_session_id),
        -- sessions.* except (user_id, session_id),

        {% for dim in dimensions %}
            if(sessions.user_id is not null, sessions.{{ dim }}, users.{{ dim }}) as {{ dim }},
        {%- endfor %}

    from signups

    left join sessions
        on signups.user_id = sessions.user_id
        and signups.max_session_id = sessions.session_id
    
    -- if both the user_id and session_id match - great, use it. otherwise, try to match a user_id without a session_id
    full outer join users
        on signups.user_id = users.user_id
)
select * from merged qualify row_number() over (partition by record_id order by signup_start_date desc) = 1


-- where user_id = 'y2pYlVjUqGjHJDPEiwklbWTUux4vgzw6aoGw0Ek0LgLihnmpc1wefu'
-- where user_id = '0Urie4dlNAqhCG22b6ERJl9QcEbZmHbzsEYnCTj3hfbHJHVvMYFaSJ'
-- where user_id = 'LhlPk53Sifpbbizk6CcIU5NSOFHsmDtbRgUtHl1jMv9M58f1OhnglQ'
-- /*

-- select user_id, signup_platform, signup_type, signup_attempt, count(*) c
-- from merged
-- group by all
-- having c > 1
-- order by c desc /*

-- where signups.user_id = 'Ee5XefDtfrjw3u3h3VnZR5PtPVBebY0xhfNRWKy1Ar0HAB54Mmjr3o'
-- order by signups.user_id, signup_start_date

/**/