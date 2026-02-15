{% macro snc_target(country_column='geo.country') %}
CASE
    WHEN {{ country_column }} IN ('Morocco', 'Saudi Arabia', 'United Arab Emirates', 'Bahrain', 'Indonesia', 'Azerbaijan', 'Libya' ) THEN 1
    ELSE 0
END AS `snc_target`,
{% endmacro %}