{# {% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %} #}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# {{ log("Running generate_schema_name(): " ~ target.name ~ ", " ~ target.schema ~ ", " ~ custom_schema_name, info=True) }} #}

    {%- if target.name == "prod" -%}
    
        {{ custom_schema_name | trim }}

    {%- elif custom_schema_name is none or custom_schema_name == '' -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}