{%- macro marketing_dimensions(dimensions=var("ga4_marketing_dimensions"), prefix='', suffix='', table='', except=[]) -%}
        {{ wrap_columns(dimensions | reject('in', except) | list, prefix, suffix, table) }}
{%- endmacro -%}