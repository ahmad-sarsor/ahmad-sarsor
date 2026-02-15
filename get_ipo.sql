{% macro get_ipo() %}
    with events as (
    select 
    id as event_id,
    entity_id,
    event_type,
    was_acquired,
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
    visibility_visibility_type,
    amount_visibility_visibility_type,
    funding_type_visibility_visibility_type
    from {{ ref('stg_mysql__base_event') }} 
    where event_type in ('POEvent','SpacEvent','ReverseMergerEvent') 
                    or (event_type like 'MNAEvent' and was_acquired = 1) 
    ),
    
    companies as (
            select entity_id,
           entity_name,
           entity_type,
           entity_sub_type,
           entity_country ,
           is_israeli,
           primary_sector_name ,
           disclosure_level,

    from {{ ref('int_entities') }}
    )
    select 
    event_id,
    e.entity_id,
    --c.entity_name,
    event_type,
    was_acquired,
    event_date,
    ticker,
    stock_exchange_key,
    source,
    capital_raised,
    valuation,
    created_date as event_created_date,
    creator_key as event_creator_key,
    updater_key as event_updater_key,
    updated_date as event_updated_date,
    visibility_visibility_type as disclosure_event_level,
    amount_visibility_visibility_type as disclosure_amount_level,
    funding_type_visibility_visibility_type as disclosure_event_type_level,
    case when dense_rank() over (partition by e.entity_id order by event_date ) = 1 and e.event_type in ('POEvent','SpacEvent','ReverseMergerEvent') then 1 else 0 end as is_ipo,
    case when dense_rank() over (partition by e.entity_id order by event_date ) = 1 and e.event_type like 'POEvent' then 1 else 0 end as is_first_ipo,

    from events as e 
        inner join companies as c on c.entity_id =e.entity_id

    qualify dense_rank() over (partition by e.entity_id order by event_date ) = 1 and e.event_type in ('POEvent','SpacEvent','ReverseMergerEvent')
    and c.is_israeli=1 and c.entity_type like 'Company'

{% endmacro %}