{%- macro url_page_path_and_query(column='page_location') -%}
    lower(regexp_extract(page_referrer, r'https?://[^/]+(/[^?#]*)')) as referrer_path,
    replace(lower(regexp_extract({{ column }}, r'https?://[^/]+(/[^?#]*)')), '//', '/') as page_path,
    trim(replace(replace(split(replace({{ column }}, '%3F', '?'), '?')[safe_offset(1)], '%3B', ';'), '&amp;', '&'), '&') as page_query,
    lower(regexp_extract({{ column }}, r"[&\?](?:section|tab)=([^\?&=]+)")) as tab,
    lower(regexp_extract({{ column }}, r"[&\?]?view=([^\?&=]+)")) as view,
    regexp_extract({{ column }}, r'/\w+_page/([^/#?]+)') as company_url_name,

{% endmacro %}

{%- macro grouped_page_path_and_name(column='page_path') -%}
    case
        when {{ column }} like '/reports/%'  then '/reports'
        when {{ column }} like '/messaging%' then '/messaging'
        when {{ column }} like '/startups/search%' then '/startups/search'
        when {{ column }} like '/company/search%' then '/startups/search'
        when {{ column }} like '/company_page/%' then '/company_page'
        when {{ column }} like '/company/%' then '/company_page'
        when {{ column }} like '/user/savedsearches' then '/user/savedsearches'
        when {{ column }} like '/about-us' then '/about/about-us'
        when {{ column }} like '/investors/search%' then '/investors/search'
        when {{ column }} like '/investor_page/%' then '/investor_page'
        when {{ column }} like '/investor/%' then '/investor_page'
        when {{ column }} like '/program_page/%' then '/program_page'
        when {{ column }} like '/mnc_page/%' then '/mnc_page'
        when {{ column }} like '/user/profile%' then '/user/profile'
        when {{ column }} like '/user/smartlist/%' then '/user/smartlist'
        when {{ column }} like '/user/watchlist/%' then '/user/watchlist'
        when {{ column }} like '/watchlist/%' then '/watchlist'
        when {{ column }} like '/hubs/search%' then '/hubs/search'
        when {{ column }} like '/multinationals/search%' then '/multinationals/search'
        when {{ column }} like '/multinational/search%' then '/multinationals/search'
        when {{ column }} like '/centralhub/%' then '/centralhub'
        when {{ column }} is null or {{ column }} = '' or {{ column }} = '/' then '/'
        else concat('/', split({{ column }}, '/')[safe_offset(1)])
    end as grouped_{{ column }},

    case
        when {{ column }} like '/reports/%'  then 'Reports'
        when {{ column }} like '/messaging%' then 'Messaging'
        when {{ column }} like '/company_page/%' then 'Company Page'
        when {{ column }} like '/user/savedsearches' then 'User Saved Searches'
        when {{ column }} like '/about-us' then 'About Us'
        when {{ column }} like '/investor_page/%' then 'Investor Page'
        when {{ column }} like '/program_page/%' then 'Program Page'
        when {{ column }} like '/mnc_page/%' then 'MNC Page'
        when {{ column }} like '/user/profile%' then 'User Profile'
        when {{ column }} like '/user/smartlist/%' then 'Smart List (User)'
        when {{ column }} like '/user/watchlist/%' then 'Watchlist (User)'
        when {{ column }} like '/watchlist/%' then 'Watchlist'
        when {{ column }} like '/company/search%' then 'Startups Search'
        when {{ column }} like '/startups/search%' then 'Startups Search'
        when {{ column }} like '/hubs/search%' then 'Hubs Search'
        when {{ column }} like '/multinationals/search%' then 'Multinational Search'
        when {{ column }} like '/multinational/search%' then 'Multinational Search'
        when {{ column }} like '/investors/search%' then 'Investors Search'
        when {{ column }} like '/centralhub/%' then 'Central Hub'
        when {{ column }} is null or {{ column }} = '' or trim({{ column }}) = '/' then 'Homepage'
        else initcap(split({{ column }}, '/')[safe_offset(1)])
    end as grouped_{{ column.removesuffix('_path') }}_name,
{%- endmacro -%}