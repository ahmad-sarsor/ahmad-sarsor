{%- macro ga4_event_ids(dimensions=var("ga4_event_ids"), prefix='', suffix='', table='', except=[]) -%}
        {{ wrap_columns(dimensions | reject('in', except) | list, prefix, suffix, table) }}
{%- endmacro -%}