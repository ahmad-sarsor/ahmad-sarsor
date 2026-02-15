{{ config(
    materialized='view',
    schema=var('intermediate_schema'),
    unique_key='record_id',
    tags=['product_analytics']
) }}

with source as (
    select
        user_id,
        is_user_id_generated,

        user_pseudo_id,
        is_user_pseudo_id_generated,

        event_datetime_utc,

        case
            when event_name = 'profile_data'
            and event_key = 'platform'
                then event__string_value
        end as signup_platform,

        case
            when event_key = 'action'
            and event__string_value = 'Sign Up'
                then event_datetime_utc
        end as signup_start_datetime,

        case
            when event_name = 'profile_data'
            and event_key = 'step'
                then event__string_value like '%complete%'
        end as is_signup_complete,

        case
            when event_name = 'profile_data'
            and event_key = 'step'
            and event__string_value like '%complete%'
                then event_datetime_utc
        end as signup_complete_datetime,

        {{ ga4_event_dimensions() }}

        {{ marketing_dimensions(except=['channel_grouping']) }}

    from {{ ref('stg_ga4__events_unnested') }}
)

, ga4_users as (
    select
        user_id,
        is_user_id_generated,

        user_pseudo_id,
        is_user_pseudo_id_generated,

        max(signup_start_datetime) as signup_start_datetime,

        array_agg(
            signup_platform
            ignore nulls
            order by event_datetime_utc
            limit 1
        )[safe_offset(0)] as signup_platform,

        coalesce(logical_or(is_signup_complete), false) as is_signup_complete,

        max(signup_complete_datetime) as signup_complete_datetime,

        {{ ga4_event_dimensions_agg(func='max_by', func_param='event_datetime_utc') }}

        {{ ga4_event_dimensions_agg(dimensions=var("ga4_marketing_dimensions"), func='min_by', func_param='event_datetime_utc', except=['channel_grouping']) }}

    from source

    group by all
)

, entity_analysis as (
    select
        user_id,
        entity_analysis_type,
        entity_analysis_sector,

    from {{ ref('int_entity_analysis') }}
)

, user_to_company as (
    select
        user_id,
        company_id,

    from {{ ref('stg_mysql__new_member') }}

    qualify row_number() over(
        partition by user_id
        order by if(company_id is not null, 0, 1), created_date desc
    ) = 1
)
-- select * from user_to_company /*

, mysql_users as (
    select
        user.user_id,
        user.created_date as user_created_date,
        -- user.username,
        user.first_name as user_first_name,
        user.last_name as user_last_name,
        coalesce(user.gender, user.user_gender) as user_gender,
        user.signup_date as user_signup_date,
        user.original_signup_date,
        user.is_registered,
        user.is_confirmed,
        user.is_snc_employee,
        user.email as user_email,
        user.business_email as user_business_email,
        user.additional_email as user_additional_email,
        user.is_email_validated,
        user.is_password_empty,
        user.linkedin_profile_url,
        user.user_type,
        user.primary_usage as user_primary_usage,

        user_to_company.user_id is not null as is_member,

        company_1.company_id,
        company_1.company_name,
        company_1.company_url_name,
        company_1.company_type,
        company_1.company_subtype,
        company_1.company_primary_sector,
        company_1.company_primary_sector_parent,

        -- coalesce(company_1.company_id, company_2.company_id) as company_id,

        -- case
        --     when company_1.company_type is not null
        --         then coalesce(company_1.company_name, user.company_name)
        --     when company_2.company_type is not null
        --         then company_2.company_name
        --     -- else user.company_name
        -- end as company_name,

        -- coalesce(company_1.company_url_name, company_2.company_url_name) as company_url_name,
        -- coalesce(company_1.company_type, company_2.company_type) as company_type,
        -- coalesce(company_1.company_subtype, company_2.company_subtype) as company_subtype,

        -- coalesce(
        --     company_1.company_primary_sector,
        --     company_2.company_primary_sector
        -- ) as company_primary_sector,

        -- coalesce(
        --     company_1.company_primary_sector_parent,
        --     company_2.company_primary_sector_parent
        -- ) as company_primary_sector_parent,

    from {{ ref('stg_mysql__new_user') }} user

    left join user_to_company
        on user.user_id = user_to_company.user_id

    left join {{ ref('int_companies') }} company_1
        on user_to_company.company_id = company_1.company_id

    -- left join {{ ref('int_companies') }} company_2
    --     on user.company_id = company_2.company_id
        -- or user.company_name = company_1.company_name
)
-- select * from mysql_users /*
-- select * from mysql_users where company_id is not null /*
-- select count(*) c, countif(company_id is not null), countif(company_name is not null) from mysql_users /*
-- select * from mysql_users where company_id is not null /*
-- select
--     user.user_id,
--     user_to_company.*,
--     company_1.*,
    
--     from {{ ref('stg_mysql__new_user') }} user 
--     left join user_to_company
--         on user.user_id = user_to_company.user_id

--     left join {{ ref('stg_mysql__new_company') }} company_1
--         on user_to_company.company_id = company_1.company_id

--     where user_to_company.company_id is not null
--     -- where company_1.company_id is not null
-- /*

, merged as (
    select
        coalesce(
            mysql_users.user_id,
            ga4_users.user_id,
            '{{ var("empty_user_id") }}'
        ) as user_id,

        coalesce(is_user_id_generated, false) as is_user_id_generated,

        coalesce(
            ga4_users.user_pseudo_id,
            '{{ var("empty_user_pseudo_id") }}'
        ) as user_pseudo_id,

        coalesce(is_user_pseudo_id_generated, false) as is_user_pseudo_id_generated,

        coalesce(is_registered, false) as is_user_registered,

        coalesce(is_confirmed, false) as is_user_confirmed,

        date(coalesce(
            mysql_users.user_signup_date,
            ga4_users.signup_complete_datetime
        )) as user_signup_date,

        coalesce(
            mysql_users.user_signup_date,
            ga4_users.signup_complete_datetime
        ) as user_signup_datetime,

        -- coalesce(case
        --     when signup_platform in ('google', 'Google One-Tap', 'linkedin')
        --         then is_signup_complete
        --     when mysql_users.original_signup_date is not null or signup_platform = 'email'
        --         then is_password_empty is true or is_email_validated is true
        -- end, false) as is_user_registered,

        ga4_users.* except (user_id, user_pseudo_id, is_user_id_generated, is_user_pseudo_id_generated),--, is_signup_complete),
        mysql_users.* except (user_id, user_signup_date, original_signup_date),

    from mysql_users
    
    full outer join ga4_users
        on mysql_users.user_id = ga4_users.user_id
)

, filtered as (
    select *

    from merged

    where not (
        user_id = '{{ var("empty_user_id") }}'
        and user_pseudo_id = '{{ var("empty_user_pseudo_id") }}'
    )

    qualify row_number() over (
        partition by user_id, user_pseudo_id
        order by
            is_user_registered desc,
            is_user_confirmed desc,
            user_signup_datetime
    ) = 1
)

, with_entity_analysis as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'filtered.user_id',
            'filtered.user_pseudo_id'
        ]) }} as record_id,

        filtered.* except (is_member),

        coalesce(filtered.is_member, false) as is_member,

        {{ channel_grouping() }}

        {{ snc_region('geo__country') }}

        {{ snc_target('geo__country') }}

        geo__country = 'Israel' as israeli,

        entity_analysis.entity_analysis_type,
        entity_analysis.entity_analysis_sector,

        rand() as _rand,

    from filtered

    left join entity_analysis
        on filtered.user_id = entity_analysis.user_id

    -- where filtered.user_id != '-1'
)

, with_derived_user_type as (
    select
        * except (_rand),

        case
            when company_type is not null then company_type

            when entity_analysis_type in (
                'Media',
                'Government'
            ) then entity_analysis_type

            when entity_analysis_type in (
                'Startup',
                'Software Development'
            ) then 'Startup'

            when entity_analysis_type = 'Corporate' then 'Corporation'

            when entity_analysis_type in (
                'Consulting Firm',
                'Law Firm'
            ) then 'Consultant or service provider'

            when entity_analysis_type = 'Academia' then 'Academia'

            when entity_analysis_type in (
                'VC',
                'Investor',
                'Private Equity',
                'Crowdfunding',
                'Corporate VC',
                'Venture Capital',
                'Angel'
            ) then 'Investor'

            when entity_analysis_type in (
                'Media',
                'Communities'
            ) then 'Media'

            when entity_analysis_type in (
                'Government',
                'Government-led'
            ) then 'Government'

            when entity_analysis_type in (
                'Non-Profit',
                'Nonprofit',
                'Non-profit',
                'Nonprofit organization'
            ) then 'NGO'

            when entity_analysis_type in (
                'Accelerator',
                'Incubator',
                'Grant Providers'
            ) then 'Hub'

            when user_type in (
                'Startup',
                'Consultant or service provider',
                'Investor',
                'Corporation',
                'Government',
                'NGO',
                'Academia',
                'Hub',
                'Media',
                'SME',
                'Job seeker'
            ) then user_type

            when user_primary_usage = 'Search Investors'
                then 'Startup'

            when user_primary_usage = 'Find Startups To Invest'
                then 'Investor'

            when user_primary_usage = 'Collaborate'
                then case
                    when _rand < 0.4 then 'Consultant or service provider'
                    when _rand < 0.7 then 'Corporation'
                    else 'Startup'
                end

            when user_primary_usage = 'Research and Analysis'
                then case
                    when _rand < 0.6 then 'Consultant or service provider'
                    else 'Investor'
                end

            when user_primary_usage = 'Search Tech Solution'
                then case
                    when _rand < 0.28 then 'Corporation'
                    when _rand < 0.54 then 'Consultant or service provider'
                    when _rand < 0.79 then 'Investor'
                    else 'Government'
                end

            when user_primary_usage = 'Search Job'
                then 'Job seeker'

            else 'Other'

        end as derived_user_type

    from with_entity_analysis
)
-- select derived_user_type, count(*) c from with_derived_user_type group by all order by 1 /*

, derived_user_type_map as (
    select
        * except (derived_user_type),

        case
            when derived_user_type = 'Job seeker' then 'Job Seeker'
            when derived_user_type = 'Startup' then 'Startup'
            when derived_user_type = 'Investor' then 'Investor'
            when derived_user_type = 'Consultant or service provider' then 'Service Provider'
            when derived_user_type = 'Corporation' then 'Corporate'
            when derived_user_type = 'Academia' then 'Academia'
            when derived_user_type = 'Government' then 'Public Sector'
            when derived_user_type = 'Hub' then 'Innovation Hub'
            when derived_user_type = 'SME' then 'Corporate'
            when derived_user_type = 'Media' then 'Media'
            when derived_user_type = 'Multinational' then 'Corporate'
            when derived_user_type = 'NGO' then 'Public Sector'
            else derived_user_type
        end as derived_user_type,

    from with_derived_user_type
)
-- select derived_user_type, count(*) c from derived_user_type_map group by all order by 1 /*

select * from derived_user_type_map /*

/**/