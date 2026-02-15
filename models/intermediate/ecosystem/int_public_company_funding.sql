{{ config(
    materialized="view",
    schema=var('intermediate_eco_schema'),
    tags=['ecosystem']
) }}

{#-
    int_public_company_funding
    ==========================
    Public company funding events: IPOs, SPACs, Reverse Mergers,
    PIPEs, and Convertible Bonds.
    
    Identifies exits and initial public offerings.
-#}

-- ============================================================================
-- IMPORTS: Staging References
-- ============================================================================

with events as (
    select 
        id as event_id,
        entity_id,
        event_type,
        funding_type_funding_type,
        event_date,
        ticker,
        stock_exchange_key,
        source,
        capital_raised,
        valuation,
        created_date,
        creator_key,
        updater_key,
        updated_date,
        visibility_visibility_type as disclosure_event_level,
        amount_visibility_visibility_type as disclosure_amount_level,
        funding_type_visibility_visibility_type as disclosure_event_type_level
    from {{ ref('stg_mysql__base_event') }} 
    where event_type in ('POEvent', 'SpacEvent', 'ReverseMergerEvent') 
        or (event_type like 'FundingRoundEvent' 
            and funding_type_funding_type in ('PIPE', 'Convertible Bond'))
        and is_silent_round = 0
),

exits as (
    {{ get_exit_events() }}
),

ipos as (
    {{ get_ipo() }}
),

users as (
    select 
        user_id,
        email,
        first_name,
        last_name,
        first_name || ' ' || last_name as user_full_name
    from {{ ref('stg_mysql__new_user') }}
),

-- ============================================================================
-- TRANSFORMATION: Public Funding Events
-- ============================================================================

public_fundings as (
    select 
        events.event_id,
        case 
            when events.event_type like 'FundingRoundEvent' then funding_type_funding_type
            else events.event_type 
        end as event_type,
        events.event_date,
        events.entity_id as company_id,
        ifnull(exits.is_exit, 0) as is_exit,
        if(events.event_id in (select event_id from ipos), 1, 0) as is_ipo,
        events.capital_raised,
        events.stock_exchange_key,
        events.ticker,
        events.valuation,
        events.disclosure_event_level,
        events.disclosure_amount_level,
        events.disclosure_event_type_level,
        events.source,
        events.created_date,
        events.creator_key,
        events.updater_key,
        events.updated_date
    from events
    left join exits on events.event_id = exits.event_id
    left join ipos on events.event_id = ipos.event_id
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

final as (
    select 
        public_fundings.event_id,
        public_fundings.event_type,
        case 
            --when event_id in (select event_id from ipos where event_type = 'POEvent') then 'Initial Public Offering'
            when event_type = 'POEvent'  then 'Public Offering'
            when event_type = 'Convertible Bond' then 'Convertible Bond'
            when event_type = 'ReverseMergerEvent' then 'Reverse Merger'
            when event_type = 'SpacEvent' then 'SPAC'
            else event_type 
        end as event_type_final,
        public_fundings.event_date,
        extract(year from public_fundings.event_date) as event_year,
        public_fundings.company_id,
        public_fundings.is_exit,
        public_fundings.is_ipo,
        public_fundings.capital_raised,
        public_fundings.stock_exchange_key,
        public_fundings.ticker,
        public_fundings.valuation,
        public_fundings.source,
        public_fundings.disclosure_event_level,
        public_fundings.disclosure_event_type_level,
        public_fundings.disclosure_amount_level,
        public_fundings.created_date as event_created_date,    
        users_creator.email as event_creator_email,
        users_creator.first_name as event_creator_first_name,
        users_creator.last_name as event_creator_last_name,
        public_fundings.updated_date as event_updated_date,
        users_updater.email as event_updater_email,
        users_updater.first_name as event_updater_first_name,
        users_updater.last_name as event_updater_last_name,
        concat(users_creator.first_name, ' ', users_creator.last_name) as event_creator_name,
        concat(users_updater.first_name, ' ', users_updater.last_name) as event_updater_name
    from public_fundings
    left join users as users_creator on public_fundings.creator_key = users_creator.user_id
    left join users as users_updater on public_fundings.updater_key = users_updater.user_id
)

select * from final