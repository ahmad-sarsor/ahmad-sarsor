{%- macro wrap_columns(columns, prefix='', suffix='', table='', source_rename={}, target_rename={}, omit_comma=False) -%}
{%- for column in columns %}
        {%- if table -%}{{ table }}.{%- endif -%}{{ source_rename.get(column, column) }}
        {%- if prefix or suffix %} as {{ prefix }}{{ target_rename.get(column, column) }}{{ suffix }}{%- endif -%}
        {%- if not (loop.last and omit_comma) -%},{%- endif %}
{% endfor -%}
{%- endmacro -%}