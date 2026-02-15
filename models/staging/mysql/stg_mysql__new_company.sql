{{ config(
    materialized='view',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

with source as (
    select * from {{ source('mysql', 'New_Company') }}
)

select
    id as company_id,
    creator_id,
    nullif(created_date, date('1970-01-01')) as created_date,
    updator_id,
    nullif(update_date, date('1970-01-01')) as updated_date,
    last_modifier_id,
    nullif(last_modification_date, date('1970-01-01')) as modified_at,
    member_verified_data, -- 0 or null
    member_verified_data_id,
    nullif(member_verified_data_date, date('1970-01-01')) as member_verified_data_date,
    bi_verify as bi_verify_key,
    nullif(bi_verify_date, date('1970-01-01')) as bi_verify_date,
    register_id,-- Need to handle non-numeric data
    user_key,
    old_key,
    tto_key,

    nullif(`name`, '') as company_name,
    nullif(`description`, '') as company_description,
    description_edited,
    description_no_html_tags,
    corona_description,
    climatetech_description,
    nullif(tag_line, '') as tag_line,
    coalesce(tag_line_edited, 0) as tag_line_edited,
    url_name as company_url_name,
    full_url,
    nullif(homepage, '') as homepage,
    nullif(careerspage, '') as careerspage,
    nullif(teamspage, '') as teamspage,

    email_domain,
    headquarter_address,
    nullif(phone, '') as phone,
    logo_file_name,
    --case when type='Startup' then 'Israel' else country end as country,
    case when type='Startup' then 'Israel' else country end as old_country,
    {{ country_mapping("case when type='Startup' then 'Israel' else country end") }} as country,
    {{ country_iso_code("case when type='Startup' then 'Israel' else country end") }} as country_iso_code,
    {{ is_valid_country("case when type='Startup' then 'Israel' else country end") }} as is_valid_country,
    case when (country ='Israel' or type='Startup') then 1 else 0 end as is_israeli,
    --case when (country ='Israel' or type='Startup') then 1 else 0 end as is_israeli,
    in_israel_since as in_israel_since_year,
    in_israel_since_month,
    num_of_israeli_acquisitions,

    primary_sector_key,
    nullif(sector, '') as company_sector,
    sector_verify as sector_verify_key,
    nullif(sector_verify_date, date('1970-01-01')) as sector_verify_date,
    category,

    nullif(stage, '') as stage,
    nullif(public_stage, '') as public_stage,
    nullif(product_stage, '') as product_stage,
    stage_of_development,
    nullif(stage_of_development_text, '') as stage_of_development_text,
    confidence_score,
    confidence_score_reason,
    founded_year,
    nullif(founded_month, 0) as founded_month,
    closing_year,
    closing_month,
    nullif(employees, '') as employees, -- range
    employees_exact,
    nullif(employees_overall, '') as employees_overall, -- range
    employees_overall_exact,

    raised,
    publicly_raised,
    capital_managed,

    market_capital,
    market_capital_year,
    nullif(market_cap_date, date('1970-01-01')) as market_cap_date,

    acquired,
    acquired_amount,
    nullif(acquired_by, '') as acquired_by,
    acquired_by_key,
    if (acquired_year >= 1980, acquired_year, null) as acquired_year,
    acquired_month,
    nullif(acquired_country, '') as acquired_country,
    nullif(acquired_added_date, date('1970-01-01')) as acquired_added_date,

    r_d_due_to_acquisition,
    r_d_due_to_acquisition_name,
    r_d_due_to_acquisition_key,

    vertical, -- 0 or 1

    nullif(exposure_date, date('1970-01-01')) as exposure_date,
    nullif(university_founded_date, date('1970-01-01')) as university_founded_date,

    nullif(`status`, '') as `status`,
    nullif(full_status, '') as full_status,
    active,
    is_available,
    coalesce(claimed, 0) as claimed,
    nullif(claimed_date, date('1970-01-01')) as claimed_date,
    coalesce(minimal_profile, 0) as minimal_profile,
    coalesce(visible, 1) as visible,
    hide_reason,
    coalesce(dont_publish, 0) as dont_publish,
    coalesce(dont_publish_reasons_not_israeli, 0) as dont_publish_reasons_not_israeli,
    coalesce(dont_publish_reasons_founder_request_to_remove, 0) as dont_publish_reasons_founder_request_to_remove,
    coalesce(dont_publish_reasons_service_provider, 0) as dont_publish_reasons_service_provider,
    coalesce(dont_publish_reasons_stealth_mode, 0) as dont_publish_reasons_stealth_mode,
    dont_publish_reasons_other,

    card_photo_title,
    card_photo_type,
    card_photo_file_name,
    card_photo_thumbnail_file_name,
    card_photo_cover_strip_index,
    card_photo_show_in_cover_strip,
    nullif(card_photo_created_date, date('1970-01-01')) as card_photo_created_date,

    cover_photo_title,
    cover_photo_type,
    cover_photo_file_name,
    cover_photo_thumbnail_file_name,
    cover_photo_cover_strip_index,
    cover_photo_show_in_cover_strip,
    nullif(cover_photo_created_date, date('1970-01-01')) as cover_photo_created_date,
    nullif(supporting_arm,'') as supporting_arm,
    nullif(investment_upper_amount,0) as investment_upper_amount,
    nullif(investment_lower_amount,0) as investment_lower_amount,
    program,
    total_graduated,--not good
    academy_technology_use_type, --idk
    investor_arm_name,
    investor_arm,
    nullif(academic_institution,'') as academic_institution,
    nullif(funding_type,'') as funding_type, -- need to check
    nullif(nullif(crunchbase_id,''),'--') as crunchbase_id,
    coalesce(takes_equity,0) as takes_equity,
    provides_office as office_space,
    initcap(membership_type) as membership_type,
    community_type,
    nullif(external_events_url,'') as external_events_url,
    nullif(nullif(technology_code,''),' ') as technology_code,
    has_fee,
    nullif(program_type,'') as program_type,
    coalesce(academia_supported,0) as academia_supported,
    nullif(supporting_arm_name,'') as supporting_arm_name,
    number_of_members, -------- need to check
    class_size as batch_size,
    nullif(headquarter_address,'') as headquarter_city,
    mail_address, -------- need to check
    duration,
    patent,
    need,
    nullif(applications,'') as applications,
    number_of_classes as batches_year,
    is_climatetech_relevant,
    type as company_type,
    nullif(sub_type,'') as company_subtype,

{#     CASE
        WHEN `type` IN (
            'Accelerator', 'Corporate Accelerator', 
            'Co-Working Space', 'Community', 
            'Entrepreneurship Program', 'General', 
            'Professional', 'Alumni', 'Investors', 
            'Founders', 'Sector', 'Regional'
        ) THEN 'Hub'
        
        WHEN `type` IN (
            'Investor', 'VC', 'Corporate VC', 
            'Angel', 'Angel Group', 'VC and Private Equity', 
            'Private Equity', 'Grant Provider', 
            'Equity Crowdfunding', 'Incubator', 
            'Institutional Investor', 'Family Office', 
            'Holding Company'
        ) THEN 'Investor'
  
        WHEN type IN ('Startup', 'Multinational') THEN type
    END AS company_type,

    CASE
        WHEN type IN (
            'VC', 'Corporate VC', 'Angel', 'Angel Group', 
            'VC and Private Equity', 'Private Equity', 
            'Grant Provider', 'Equity Crowdfunding', 
            'Incubator', 'Institutional Investor', 
            'Family Office', 'Holding Company', 
            'Accelerator', 'Corporate Accelerator', 
            'Co-Working Space', 'Community', 
            'Entrepreneurship Program', 'General', 
            'Professional', 'Alumni', 'Investors', 
            'Founders', 'Sector', 'Regional'
        ) THEN type
        
        ELSE sub_type
    END AS company_subtype,
 #}
from source
where name not in('GoingAnywhere','Igal Company')

