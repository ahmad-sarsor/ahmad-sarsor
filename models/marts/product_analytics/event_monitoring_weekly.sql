-- Event Monitoring Analysis
-- This model analyzes event patterns over time, comparing recent periods to hist data
-- and identifying significant changes in event volumes and user engagement.
{{ config(
    materialized='table',
    schema=var('marts_schema'),
    tags=['product_analytics']
) }}

-- Configuration variables
{%- set lookback_days = var('lookback_days', 120) -%}
{%- set short_period_days = var('short_period_days', 7) -%}
{%- set short_period_start_date = 'DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL ' + short_period_days|string + ' DAY), DAY)' -%}
{%- set event_keys = var('event_keys', ['action', 'label', 'type']) -%}
{%- set min_event_count = var('min_event_count', 5) -%}
{%- set min_unique_users = var('min_unique_users', 5) -%}
{%- set max_value_length = var('max_value_length', 10) -%}
{%- set start_date = var('start_date', 'DATE_SUB(CURRENT_DATE(), INTERVAL ' + lookback_days|string + ' DAY)') -%}

-- Base data with date filtering
WITH base_events AS (
  SELECT *
  FROM {{ ref('stg_ga4__events_unnested') }}
  WHERE event_date >= {{ start_date }}
),

-- Aggregate all events by day and event name
daily_event_totals AS (
  SELECT
    DATE_TRUNC(event_date, DAY) AS analysis_date,
    event_name,
    CAST(NULL AS STRING) AS event_key,
    CAST(NULL AS STRING) AS event_value,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions,
    MAX(event_datetime_utc) AS last_event_time
  FROM base_events
  GROUP BY analysis_date, event_name
),

-- Aggregate events with specific key values
daily_event_details AS (
  SELECT
    DATE_TRUNC(event_date, DAY) AS analysis_date,
    event_name,
    event_key,
    event__string_value AS event_value,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions,
    MAX(event_datetime_utc) AS last_event_time
  FROM base_events
  WHERE event_key IN ({%- for key in event_keys %}'{{ key }}'{%- if not loop.last %},{% endif %}{%- endfor %})
    AND event__string_value IS NOT NULL 
    AND event__string_value != ''
    AND LENGTH(event__string_value) < {{ max_value_length }}
  GROUP BY analysis_date, event_name, event_key, event_value
),

-- Combine totals and details
all_daily_metrics AS (
  SELECT * FROM daily_event_totals
  UNION ALL
  SELECT * FROM daily_event_details
),

-- Calculate period averages and totals
period_metrics AS (
  SELECT
    event_name,
    event_key,
    event_value,
    
    -- hist period (excluding recent days)
    ROUND(
      AVG(CASE 
        WHEN analysis_date < {{ short_period_start_date }}
        THEN event_count 
        ELSE NULL 
      END), 2
    ) AS hist_avg_daily_events,
    
    SUM(CASE 
      WHEN analysis_date < {{ short_period_start_date }}
      THEN event_count 
      ELSE 0 
    END) AS hist_total_events,
    
    -- Recent period
    ROUND(
      AVG(CASE 
        WHEN analysis_date >= {{ short_period_start_date }}
        THEN event_count 
        ELSE NULL 
      END), 2
    ) AS recent_avg_daily_events,
    
    SUM(CASE 
      WHEN analysis_date >= {{ short_period_start_date }}
      THEN event_count 
      ELSE 0 
    END) AS recent_total_events

  FROM all_daily_metrics
  GROUP BY event_name, event_key, event_value
),

-- Calculate percentage changes
final_metrics AS (
  SELECT
    event_name,
    event_key,
    event_value,
    COALESCE(hist_avg_daily_events, 0) AS hist_avg_daily_events,
    hist_total_events,
    COALESCE(recent_avg_daily_events, 0) AS recent_avg_daily_events,
    recent_total_events,
    
    -- Calculate percentage change
    CASE 
      WHEN COALESCE(hist_avg_daily_events, 0) > 0 
      THEN ROUND(
        SAFE_DIVIDE(
        COALESCE(recent_avg_daily_events, 0) - COALESCE(hist_avg_daily_events, 0),COALESCE(hist_avg_daily_events, 0)
        ) * 100, 2)
      WHEN COALESCE(hist_avg_daily_events, 0) = 0 AND COALESCE(recent_avg_daily_events, 0) > 0
      THEN 100.00
      ELSE -100.00
    END AS pct_change

  FROM period_metrics
)

-- Final output with filtering
SELECT
  event_name,
  event_key,
  event_value,
  hist_avg_daily_events,
  hist_total_events,
  recent_avg_daily_events,
  recent_total_events,
  pct_change
FROM final_metrics
WHERE hist_avg_daily_events >= {{ min_event_count }}
  AND hist_total_events >= {{ min_unique_users }}
ORDER BY pct_change 
