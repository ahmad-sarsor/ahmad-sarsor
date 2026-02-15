{{ config(
    materialized="view",
    schema=var('intermediate_eco_schema'),
    tags=['ecosystem']
) }}

-- ============================================================
-- 1. IMPORTS: Staging References
-- ============================================================
with events as (
    {{ get_exit_events() }}
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

-- ============================================================
-- 2. BASE: Core Acquisition Data
-- ============================================================
acquisitions as (
    select 
    event_id,
    event_type,
    event_date,
    amount,
    acquired_id,
    acquirer_id,
    mna_type,
    source,
    created_date as event_created_date,
    creator_key,
    updater_key,
    updated_date as event_updated_date,
    was_acquired,
    is_exit,
    case when dense_rank() over (partition by acquired_id order by event_date asc) =1 then 1 else 0 end as is_first_mna
    from events as e 
    where e.event_type = 'MNAEvent' and e.was_acquired =1
)

-- ============================================================
-- 4. ENRICHMENT: Add Related Data
-- ============================================================

select acquisitions.*,
        users_creator.email as event_creator_email,
        users_creator.first_name as event_creator_first_name,
        users_creator.last_name as event_creator_last_name,
        users_updater.email as event_updater_email,
        users_updater.first_name as event_updater_first_name,
        users_updater.last_name as event_updater_last_name,
        concat(users_creator.first_name, ' ', users_creator.last_name) as event_creator_name,
        concat(users_updater.first_name, ' ', users_updater.last_name) as event_updater_name
from acquisitions as acquisitions 
    left join users as users_creator on acquisitions.creator_key = users_creator.user_id
    left join users as users_updater on acquisitions.updater_key = users_updater.user_id






