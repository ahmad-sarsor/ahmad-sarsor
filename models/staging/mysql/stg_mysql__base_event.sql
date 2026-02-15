{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

select
    id,
    company_key as entity_id,
    event_type,
    nullif(created_date, date('1970-01-01')) as created_date,
    nullif(updated_date, datetime('1970-01-01')) as updated_date,
    nullif(date, date('1970-01-01')) as event_date,
    creator_key,
    updator_key as updater_key,
    source,

    nullif(funding_type_funding_type,'') as funding_type_funding_type, -- FundingRoundEvent
    is_extension_round, -- FundingRoundEvent
    ifnull(is_silent_round,0) as is_silent_round, -- FundingRoundEvent
    funding_type_visibility_visibility_type, -- FundingRoundEvent, POEvent,

    reason, -- ClosingEvent

    other_party_visibility_visibility_type, -- MNAEvent
    mna_type, -- MNAEvent
    other_party_entity_key, -- MNAEvent

    type, -- GrantEvent
    is_joint_venture_grant, -- GrantEvent

    institution_entity_key, -- GraduationEvent
    institution_visibility_visibility_type, -- GraduationEvent
    end_date, -- GraduationEvent
    batch, -- GraduationEvent

    name, -- InvestmentFirmFundingEvent

    community_key, -- CommunityInvolvementEvent
    community_involvement_type, -- CommunityInvolvementEvent
    
    due_to_acquisition, -- DelistingEvent

    token_visibility_visibility_type, -- ICOEvent
    token, -- ICOEvent

    merged_with_visibility_visibility_type, -- SpacEvent
    merged_with_entity_key, -- SpacEvent

    visibility_visibility_type, -- almost all event types
    nullif(amount_amount,0) as amount_amount, -- FundingRoundEvent, MNAEvent, GrantEvent, InvestmentFirmFundingEvent, POEvent, ICOEvent
    amount_visibility_visibility_type, -- FundingRoundEvent, MNAEvent, GrantEvent, InvestmentFirmFundingEvent, POEvent, ICOEvent

    ticker, -- POEvent, DelistingEvent, ReverseMergerEvent, SpacEvent
    stock_exchange_key, -- POEvent, DelistingEvent, ReverseMergerEvent, SpacEvent
    is_exit, -- MNAEvent, POEvent, ReverseMergerEvent, SpacEvent
    acquired_company_entity_key, -- POEvent, ReverseMergerEvent, SpacEvent
    was_acquired, -- MNAEvent, DelistingEvent
    capital_raised, -- POEvent, SpacEvent
    valuation, -- POEvent, SpacEvent
    acquired_company_visibility_visibility_type, -- POEvent, ReverseMergerEvent

from {{ ref('ext_mysql__base_event') }}