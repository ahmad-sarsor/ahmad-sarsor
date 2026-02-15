{%- macro max_by_event_key(event_keys, max_by='event_datetime_utc', event_value='event__string_value') -%}
  {% for event_key in event_keys %}
    max_by(
        case when event_key = '{{ event_key }}' then {{ event_value }} end,
        {{ max_by }}
    ) as {{ event_key }},
  {% endfor %}
{%- endmacro -%}