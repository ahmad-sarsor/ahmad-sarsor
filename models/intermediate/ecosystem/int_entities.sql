{{ config(
    materialized="table",
    schema=var('intermediate_eco_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

{#-
    int_entities
    ============
    Central intermediate table consolidating all entity information
    from multiple sources into a single comprehensive view.
-#}

-- ============================================================================
-- IMPORTS: Staging References
-- ============================================================================

with entities as (
    select *
    from {{ ref('stg_mysql__new_company') }}
),

classification as (
    select
        classification_id,
        root_key,
        sector_name as classification_sector,
        url_name as classification_url_name
    from {{ ref('stg_mysql__base_classification_model') }}
),

users as (
    select 
        user_id,
        email,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name
    from {{ ref('stg_mysql__new_user') }}
),

members as (
    select *,
        first_name || ' ' || last_name as full_name
    from {{ ref('stg_mysql__new_member') }}
),

hide_reason as (
    select * 
    from {{ ref('stg_mysql__hide_profile_reason') }}
),

business_models as (
    select
        company_id,
        string_agg(business_model, ', ' order by business_model) as business_model
    from {{ ref('stg_mysql__new_company_business_model') }}
    where business_model is not null
    group by company_id
),

markets as (
    select
        company_id,
        string_agg(market, ', ' order by market) as geographic_markets
    from {{ ref('stg_mysql__new_company_markets') }}
    where market is not null
    group by company_id
),

common_misspellings as (
    select
        entity_id,
        string_agg(alternative_name, ', ' order by alternative_name) as common_misspellings
    from {{ ref('stg_mysql__new_company_alternative_names') }}
    group by entity_id
),
-- ============================================================================
-- EVENTS & ENGAGEMENT METRICS
-- ============================================================================

closing_events as (
    select
        entity_id,
        event_date,
        initcap(replace(reason, '_', ' ')) as closed_reason,
        --reason as closed_reason,
        row_number() over (
            partition by entity_id 
            order by event_date desc, created_date desc
        ) as row_num
    from {{ ref('stg_mysql__base_event') }}
    where event_type = 'ClosingEvent'
        and event_date is not null
        and event_date != date('1970-01-01')
),

closed_date as (
    select
        entity_id,
        event_date as closed_date,
        closed_reason
    from closing_events
    where row_num = 1
),

num_followers as (
    select
        collection_company.company_id,
        count(distinct collection.owner_id) as num_followers
    from {{ ref('stg_mysql__collection_company_relation') }} as collection_company
    inner join {{ ref('stg_mysql__collection') }} as collection
        on collection_company.collection_id = collection.collection_id
    where collection.owner_id is not null
    group by collection_company.company_id
),

num_members as (
    select
        company_id,
        ifnull(count(distinct member_id), 0) as num_members,
        ifnull(count(distinct case when claimed=1 then member_id end), 0) as num_claimed_members
    from members as members
    where members.delete_reason is null
    group by company_id
),

-- ============================================================================
-- CORPORATE STRUCTURE & HIERARCHY
-- ============================================================================

corporate_arms as (
    select
        trim(supporting_arm_name) as parent_name,
        upper(trim(supporting_arm_name)) as parent_key,
        trim(company_name) as arm_name,
        company_id as arm_id
    from entities
    where supporting_arm_name is not null
        and supporting_arm_name != ''
),

parent_companies as (
    select
        upper(trim(company_name)) as parent_key,
        array_agg(distinct company_id order by company_id)[offset(0)] as parent_id,
        array_agg(distinct trim(company_name) order by trim(company_name))[offset(0)] as parent_name
    from entities
    where company_name is not null and company_name != ''
    group by 1
),

arms_aggregated as (
    select
        p.parent_name,
        p.parent_id,
        string_agg(distinct a.arm_name, ' , ' order by a.arm_name) as arms_list,
        count(distinct a.arm_id) as arms_count
    from corporate_arms a
    left join parent_companies p using (parent_key)
    group by p.parent_name, p.parent_id
),

-- ============================================================================
-- FINAL TRANSFORMATION
-- ============================================================================

final as (
    select
        -- SECTION 1: BASIC ENTITY INFORMATION
        entities.company_id as entity_id,
        entities.company_name as entity_name,
        entities.company_url_name as entity_url_name,
        entities.full_url as entity_full_url,
        concat ("https://finder.startupnationcentral.org", entities.full_url) as entity_finder_url,
        entities.company_description as entity_description,
        entities.tag_line as entity_tagline,
        case when entities.company_type like 'Startup' then 'Company'
             else entities.company_type end as entity_type,
        entities.company_subtype as entity_sub_type,
        entities.supporting_arm as representing_of_id,
        entities.supporting_arm_name as representing_of_name,
        entities.investor_arm,
        entities.investor_arm_name,
        
        -- SECTION 2: BUSINESS CLASSIFICATION & SECTOR INFORMATION
        classification.classification_sector as primary_sector_name,
        {{ get_sector_short_name('classification.classification_sector') }} as primary_sector_short_name,
        nullif(
            root_classification.classification_sector,
            classification.classification_sector
        ) as primary_sector_parent,
        business_models.business_model,
        common_misspellings.common_misspellings,
        entities.primary_sector_key,
        entities.category as academia_category,
        
        -- SECTION 3: BUSINESS STAGE & DEVELOPMENT
        entities.stage as internal_funding_stage,
        entities.public_stage as public_funding_stage,
        entities.product_stage as product_stage,
        entities.status as entity_status,
        entities.full_status as entity_full_status,
        entities.active as is_active,
        
        -- SECTION 4: GEOGRAPHIC INFORMATION
        entities.country as entity_country,
        entities.is_israeli as is_israeli,
        entities.in_israel_since_year,
        entities.in_israel_since_month,
        entities.headquarter_address,
        
        -- SECTION 5: FINANCIAL & INVESTMENT DATA
        entities.founded_year,
        entities.founded_month,
        case 
            when entities.founded_year is not null and entities.founded_month is not null 
                then date(entities.founded_year, entities.founded_month, 1)
            when entities.founded_year is not null 
                then date(entities.founded_year, 1, 1)
            else null 
        end as founded_date,
        closed_date.closed_date,
        extract(year from if(active=0,closed_date.closed_date,null)) as closed_year,
        extract(month from if(active=0,closed_date.closed_date,null)) as closed_month,
        closed_date.closed_reason,
        entities.raised as internal_amount_raised,
        entities.publicly_raised as public_amount_raised,
        entities.capital_managed as capital_managed,
        entities.market_capital as market_cap,
        arms_aggregated.arms_list as arms,
        entities.market_cap_date as market_cap_update,
        markets.geographic_markets,
        
        -- SECTION 6: OPERATIONAL DATA
        entities.employees as israeli_employees_range,
        entities.employees_exact as israeli_employees_exact,
        entities.employees_overall as global_employees_range,
        entities.employees_overall_exact as global_employees_exact,
        num_members.num_members, 
        num_members.num_claimed_members,
        coalesce(num_followers.num_followers, 0) as num_followers,
            
        -- Digital Presence
        entities.homepage as entity_website,
        entities.careerspage as entity_careers_page,
        entities.teamspage as entity_teams_page,
        entities.email_domain as entity_email_domain,
        
        -- Media Assets
        entities.logo_file_name as entity_logo,
        concat ("https://storage.googleapis.com/clean-finder-353810/", entities.logo_file_name) as entity_logo_url,
        
        -- SECTION 7: BUSINESS METRICS & SCORES
        entities.confidence_score,
        entities.confidence_score_reason,
        
        -- SECTION 8: PLATFORM & METADATA
        entities.creator_id as creator_id, 
        users.email as creator_email,
        users.full_name as creator_name,         
        entities.created_date as created_date,
        entities.updator_id as updater_id,
        entities.updated_date as updated_date,
        entities.last_modifier_id,
        entities.modified_at as last_modified_date,
        
        -- Verification & BI Data
        entities.bi_verify_key,
        bi_verify_user.email as bi_verify_email,
        bi_verify_user.full_name as bi_verify_name,
        entities.bi_verify_date,
        entities.sector_verify_key,
        entities.sector_verify_date,
        
        -- Platform Management
        entities.claimed as is_claimed,
        if(claimed=0,null,entities.claimed_date) as claimed_date,
        entities.minimal_profile as is_minimal_profile,
        
        -- Legacy Keys & External IDs
        entities.register_id,
        entities.crunchbase_id,
        entities.user_key,
        angel_users.email as angel_user_email,
        angel_users.full_name as angel_user_name,
        entities.hide_reason as hide_reason_id,
        coalesce(reason_description, hide_reason_id) as hide_reason,
        case when  hide_reason_id is null  then 'all'
             when (hide_reason_id in ('ddgjKuc3GgH8E5UehSVJviRl471BCajL1plO8HtGoOojy2NivYqQ7x',
                                      'J7fQJtnT3GdWWSE3MD8rzaW65LJ4jEIKxSZnneQiReONdZjG3zIqWe',
                                      'o9Wzo8oG7DIeZVpiC2CdK6IVPWAA4wFvEE97ITVY0oHo6HyeP4v2YV',
                                      'wcXeLKSl465vLdJtWXjddE1UVQbANeL8NjdsDpAh0CYTtOJw8wHP97',
                                      'uMEVRdB2yMstoSlojEH9yjleK8xuogo1ohkJ2UVVdSbrGT6uhGOOTO',
                                      '3P3LBMOo0qLbkFJSYom5ZHiJ3EkB5s7Sh8LLZzEu4qJCFuTIYnRFJj',
                                      'S0Ed46vNI6vZYVxLedANWGjas23K6hY0QdsCtiGYL7lshqIEdLV3sA',
                                      '7KVi9S7qqUtO0WCsFclwqBEZluejfNYqWC1Uhx0tPHtDxtvxPPaCPH')) then 'aggregation'
                                       else 'none' end as disclosure_level

    from entities
    left join classification as classification on entities.primary_sector_key = classification.classification_id
    left join classification as root_classification on classification.root_key = root_classification.classification_id

    left join users as users on entities.creator_id = users.user_id
    left join users as bi_verify_user on entities.bi_verify_key = bi_verify_user.user_id   
    left join users as angel_users on entities.user_key = angel_users.user_id

    left join hide_reason as hide_reason on entities.hide_reason = hide_reason.hide_reason_id
    left join closed_date as closed_date on entities.company_id = closed_date.entity_id
    left join arms_aggregated as arms_aggregated on entities.company_id = arms_aggregated.parent_id
    left join num_members as num_members on entities.company_id = num_members.company_id
    left join num_followers as num_followers on entities.company_id = num_followers.company_id
    left join business_models on entities.company_id = business_models.company_id
    left join markets on entities.company_id = markets.company_id
    left join common_misspellings on entities.company_id = common_misspellings.entity_id
)

select * from final