{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ ref('ext_mysql__new_user') }}
)
-- select
--   signup_date is not null and (password is null or password = '' or email_validated = 1) as is_registered,
--   count(distinct id)
-- from source
-- group by 1 /*

, renamed as (
    select
        id as user_id,
        -- NULLIF(username, '') as username,
        NULLIF(first_name, '') as first_name,
        NULLIF(last_name, '') as last_name,
        NULLIF(signup_date, date('1970-01-01')) as signup_date,
        signup_date as original_signup_date,
        NULLIF(user_gender, '') as user_gender,
        NULLIF(gender, '') as gender,
        NULLIF(email_for_password, '') as email_for_password,
        NULLIF(linkedin_id, '') as linkedin_id,
        NULLIF(personal_website, '') as personal_website,
        is_member,
        delivery_email_updated,
        NULLIF(old_key, '') as old_key,
        NULLIF(prefix, '') as prefix,
        last_home_page_view_date,
        is_female,
        suggested_intros_count,
        NULLIF(pending_delivery_email, '') as pending_delivery_email,
        confirmed,
        NULLIF(salesforce_id, '') as salesforce_id,
        
        NULLIF(trim(category_raw), '') as category_raw,

        case
            when
                LOWER(category_raw) in (
                    "hub", "hub (accelerator, community or similar)"
                )
                then "Hub"
            when
                LOWER(category_raw) in ("investor", "investment firm")
                then "Investor"
            when
                LOWER(category_raw) in (
                    "consultant or service provider", "consulting firm"
                )
                then "Consultant or service provider"
            when
                LOWER(category_raw) in (
                    "sme (small and medium-sized enterprises)", "sme"
                )
                then "SME"
            when
                category_raw in (
                    "Other",
                    "Startup",
                    "Corporation",
                    "NGO",
                    "Government",
                    "Academia",
                    "Media"
                )
                then category_raw
            when trim(category_raw) = "" or category_raw is null then null
            else trim(category_raw)
        end as user_type,

        NULLIF(headline, '') as headline,
        sent_intros_count,
        NULLIF(honorifics, '') as honorifics,
        NULLIF(details, '') as details,
        nullif(email, '') as email,
        updated_date,
        is_former_moderator,
        NULLIF(logo_file_name, '') as logo_file_name,
        untended_intros_count,
        is_moderator,
        NULLIF(phone, '') as phone,
        NULLIF(addtional_email, '') as additional_email,
        NULLIF(delivery_email, '') as delivery_email,
        unsubscribe,
        NULLIF(introduction_company, '') as introduction_company,
        NULLIF(password, '') as password,
        email_validated,
        salesforce_contact,
        NULLIF(chat_id, '') as chat_id,
        max_number_of_intros,
        NULLIF(linkedin_profile_url, '') as linkedin_profile_url,
        NULLIF(token, '') as token,
        created_date,
        NULLIF(introduction_email, '') as introduction_email,
        token_valid_until,
        NULLIF(introduction_position, '') as introduction_position,
        last_session_start,
        last_session_end,
        NULLIF(company_name, '') as company_name,
        NULLIF(country, '') as country,
        NULLIF(registration_referral, '') as registration_referral,
        NULLIF(business_email, '') as business_email,
        business_email_validated,
        send_to_business_email,
        case
            when primary_usage like "%search-investors%" then "Search Investors"
            when primary_usage like "%job%" then "Search Job"
            when
                primary_usage like "%research-and-analysis%"
                then "Research and Analysis"
            when primary_usage like "%collaborate%" then "Collaborate"
            when
                primary_usage like "%tech-solution%"
                then "Search Tech Solution"
            when
                primary_usage like "%find-startups-to-invest%"
                then "Find Startups To Invest"
            else null
        end as primary_usage,
        NULLIF(business_email_redirect, '') as business_email_redirect,
        NULLIF(category_raw_other, '') as category_raw_other,
        NULLIF(company_id, '') as company_id,
        NULLIF(company_website, '') as company_website,
        NULLIF(sourceuri, '') as sourceuri,

        email_validated = 1 as is_email_validated,

        password is null or password = '' as is_password_empty,

        email like '%@sncentral.org' as is_snc_employee,

        -- for platform in ('one tap', 'gmail', 'linkedin') - must have 'profile_data' step 'completed'

    from source
)

, cte as (
    select
        *,

        confirmed is not null and confirmed = 1 as is_confirmed,

        -- case
        --     when last_session_start is null
        --         then false

        --     when email is not null and password is not null
        --         then is_email_validated
            
        --     when password is null
        --         then true

        --     else false
        -- end as is_registered,

        {# confirmed is not null and confirmed = 1 #}
        original_signup_date is not null
        and (is_password_empty or is_email_validated)
            as is_registered,

        greatest(
            coalesce(last_session_start, date('1970-01-01')),
            coalesce(last_session_end, date('1970-01-01')),
            coalesce(last_home_page_view_date, date('1970-01-01')),
            coalesce(updated_date, date('1970-01-01'))
        ) as latest_active_date,

    from renamed
)
-- select * from cte /*

, filtered as (
    select * except (latest_active_date)

    from cte

    qualify row_number() over (partition by lower(user_id) order by latest_active_date desc) = 1
)
select * from filtered /*


-- select
--   is_registered,
--   count(distinct user_id) c
-- from cte
-- group by 1 /*
-- select * from cte where user_id = 'agxzfmlsbGlzdHNpdGVyFQsSCE5ld19Vc2VyGICAoNnQyZ8JDA' /*

-- select * from EXTERNAL_QUERY("finderv2.eu.cloudsql", """
--     select New_User.* from New_User 
--     right join (select distinct lower(id) id from New_User group by 1 having count(distinct first_name) > 1) a
--     on lower(New_User.id) = a.id
-- """)

/**/