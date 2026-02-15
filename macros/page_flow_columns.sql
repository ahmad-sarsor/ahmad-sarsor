{%- macro page_flow_columns(table, columns, n=4) -%}
    {%- for i in range(n) -%}
    {%- for column in columns -%}
    {%- if i == 0 -%}
        {{ column }}
    {%- else -%}
        lead({{ column }}, {{ i }}) over (partition by {{ table }}.session_id order by event_datetime_utc)
    {%- endif %} as {{ column }}_{{ i + 1 }},
    {% endfor %}
    {% endfor -%}
{%- endmacro -%}