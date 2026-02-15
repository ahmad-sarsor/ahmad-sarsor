{%- macro incremental_predicate(window_days, source_column='event_date', target_column='event_date', where_=False, and_=False, source_table=None) -%}
    {% if target.name == 'prod' -%}
        {% if is_incremental() %}
            {% if where_ %}where {% elif and_ %}and {% endif -%}
            {{ source_column }} between
            least(
                (select max({{ target_column }}) from {{ source_table or this }}),
                current_date() - interval {{ window_days }} day
            )
            and current_date()
        {% endif %}
    {% else -%}
        {% if where_ %}where {% elif and_ %}and {% endif -%}
        {{ source_column }} {{ var("dev_dates") }}
    {%- endif -%}
{%- endmacro -%}