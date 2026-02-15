{{ config(
    materialized = 'incremental',
    schema = var('mysql_staging_schema'),
    unique_key = 'id',
    partition_by = {
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by = ['action', 'user', 'modified_table'],
    tags=['product_analytics', 'ecosystem']
) }}

{%- set history_days = var("incremental_window_days") -%}

WITH modifications_with_week_start AS (
    SELECT
        *,
        DATE_TRUNC(DATE(timestamp), WEEK(SUNDAY)) AS date
    {% if target.name == 'prod' -%}
        {% if is_incremental() %}
            FROM EXTERNAL_QUERY(
                "finderv2.eu.cloudsql",
                {{ '"' ~
                "SELECT * " ~
                "FROM New_Modification " ~
                "WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL " ~ history_days ~ " DAY)"
                ~ '"' }}
            )
        {% else -%}
            FROM EXTERNAL_QUERY(
                "finderv2.eu.cloudsql",
                {{ '"' ~
                "SELECT * FROM New_Modification"
                ~ '"' }}
            )
        {% endif %}
    {% else -%}
        FROM EXTERNAL_QUERY(
            "finderv2.eu.cloudsql",
            {{ '"' ~
            "SELECT * " ~
            "FROM New_Modification " ~
            "WHERE DATE(timestamp) " ~ var('dev_dates')
            ~ '"' }}
        )
    {%- endif %}
)

SELECT
    date as  week_start_date,
    *
FROM modifications_with_week_start
