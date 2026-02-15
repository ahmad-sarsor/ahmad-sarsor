{%- macro user_dimensions(dimensions=var("user_dimensions"), prefix='', suffix='', table='', except=[]) -%}
        {{ wrap_columns(dimensions | reject('in', except) | list, prefix, suffix, table) }}
{%- endmacro -%}