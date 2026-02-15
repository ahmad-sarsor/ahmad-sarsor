{% macro get_exit_events() %}

    SELECT 
        id AS event_id,
        event_type,
        event_date,
        amount_amount AS amount,
        entity_id AS acquired_id,
        other_party_entity_key AS acquirer_id,
        mna_type,
        source,
        created_date,
        creator_key,
        updater_key,
        updated_date,
        was_acquired,
        CASE 
            WHEN dense_rank() OVER (PARTITION BY entity_id ORDER BY event_date ASC) = 1 
            THEN 1 
            ELSE 0 
        END AS is_exit,
        row_number() OVER (PARTITION BY entity_id ORDER BY event_date ASC) as exit_order
    FROM {{ ref('stg_mysql__base_event') }} 
    WHERE (event_type = 'MNAEvent' AND was_acquired = 1) 
       OR (event_type IN ('POEvent', 'SpacEvent', 'ReverseMergerEvent'))

{% endmacro %}