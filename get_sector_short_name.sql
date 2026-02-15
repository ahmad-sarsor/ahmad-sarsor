-- macros/get_sector_short_name.sql
{% macro get_sector_short_name(sector_column) %}
case {{ sector_column }}
    when 'Health Tech & Life Sciences' then 'Health Tech'
    when 'Media & Entertainment Technologies' then 'Media & Entertainment'
    when 'Industrial Technologies' then 'Industrial Tech'
    when 'Automotive & Mobility Technologies' then 'Automotive & Mobility'
    when 'Cyber Security' then 'Cybersecurity'
    when 'Aerospace, Defense & HLS' then 'Aerospace & Defense'
    when 'Agriculture & Food Technologies' then 'Agri-Food Tech'
    when 'Education & Knowledge Technologies' then 'Ed Tech'
    when 'Ag Tech' then 'Agri-Food Tech'
    when 'Climate Tech' then 'Climate Tech'
    when 'Operations Solutions' then 'Operations'
    else {{ sector_column }}  -- Energy Tech, Business Software, Retail Platforms, Fintech & Insurtech
end
{% endmacro %}