{%- macro array_to_sql_list(fields, prefix='', postfix='') -%}
  (
  {%- for element in fields -%}
    {%- if element is number -%}
      {{ element }}
    {%- else -%}
      {%- set element = prefix ~ element ~ postfix -%}
      '{{ element }}'
    {%- endif -%}

    {%- if not loop.last -%} , {% endif %}
  {%- endfor -%}
  )
{%- endmacro -%}