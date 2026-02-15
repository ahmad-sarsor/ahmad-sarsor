{% macro json_macros(schema=None) %}
{% if execute %}
  {% set query %}
    create or replace function {{ schema or target.schema }}.json_extract_keys(input string) returns array<string> language js as """
    try {
        return Object.keys(JSON.parse(input));
    } catch (e) {
        // throw new Error(input);
        return null;
    }
    """;

    create or replace function {{ schema or target.schema }}.json_extract_values(input string) returns array<string> language js as """
    try {
        return Object.values(JSON.parse(input));
    } catch (e) {
        // throw new Error(input);
        return null;
    }
    """;

    create or replace function {{ schema or target.schema }}.json_pairs(input string) returns array<struct<filter_name string, filter_value string>> language js as """
    if (!input) return [];
    try {
        const obj = JSON.parse(input);
        return Object.entries(obj).map(([k, v]) => ({
            filter_name: String(k),
            filter_value: v == null ? null : String(v)
        }));
    } catch (e) {
        return [];
    }
    """;
  {% endset %}

  {% do run_query(query) %}

{% endif %}
{% endmacro %}