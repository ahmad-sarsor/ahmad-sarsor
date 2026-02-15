{%- macro ga4_event_dimensions(dimensions=var("ga4_event_dimensions"), prefix='', suffix='', table='', except=[]) -%}
    {{ wrap_columns(dimensions | reject('in', except) | list, prefix, suffix, table) }}
{%- endmacro -%}

{#{%- macro ga4_session_dimensions(dimensions=var("ga4_event_dimensions"), prefix='', suffix='', table='') -%}
    {{ wrap_columns(dimensions, prefix, suffix, table) }}
{%- endmacro -%}#}

{%- macro ga4_event_dimensions_agg(dimensions=var("ga4_event_dimensions"), func='min_by', func_param=None, except=[]) -%}
  {%- for dim in dimensions if dim not in except -%}
    {%- if func_param %}
      {{ func }}({{ dim }}, {{ func_param }}) as {{ dim }}
    {%- else %}
      {{ func }}({{ dim }}) as {{ dim }}
    {%- endif %},
  {%- endfor %}
{%- endmacro -%}