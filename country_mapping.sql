{% macro country_mapping(country_column) %}
{#
    Standardizes country names to a consistent format.
    Handles: duplicates (US/USA), typos (Caina), invalid values (Kentucky), regions.
    
    Usage:
        {{ country_mapping('entity_country') }} as country_standardized
    
    Returns: Standardized country name or original value if no mapping found
#}
CASE
    -- United States variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('united states', 'us', 'usa', 'u.s.', 'u.s.a.', 'america') THEN 'United States'
    WHEN LOWER(TRIM({{ country_column }})) = 'kentucky' THEN 'United States'
    
    -- United Kingdom variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('united kingdom', 'uk', 'u.k.', 'great britain', 'britain', 'england') THEN 'United Kingdom'
    
    -- China variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('china', 'caina', 'prc') THEN 'China'
    
    -- Russia variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('russia', 'russian federation', 'moscow') THEN 'Russia'
    
    -- South Korea variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('south korea', 'korea', 'republic of korea', 'rok') THEN 'South Korea'
    
    -- Ireland variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('ireland', 'republic of ireland', 'eire') THEN 'Ireland'
    
    -- Czech Republic variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('czech republic', 'czechia', 'czech') THEN 'Czech Republic'
    
    -- Iceland / Island typo
    WHEN LOWER(TRIM({{ country_column }})) IN ('iceland', 'island') THEN 'Iceland'
    
    -- Swaziland -> Eswatini (official name change)
    WHEN LOWER(TRIM({{ country_column }})) IN ('swaziland', 'eswatini') THEN 'Eswatini'
    
    -- EU Brussels -> Belgium
    WHEN LOWER(TRIM({{ country_column }})) IN ('eu, brussels', 'eu brussels', 'brussels') THEN 'Belgium'
    
    -- Singapore with trailing space
    WHEN LOWER(TRIM({{ country_column }})) = 'singapore' THEN 'Singapore'
    
    -- UAE variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('united arab emirates', 'uae', 'u.a.e.', 'dubai', 'abu dhabi') THEN 'United Arab Emirates'
    
    -- Hong Kong variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('hong kong', 'hongkong', 'hk') THEN 'Hong Kong'
    
    -- Taiwan variations
    WHEN LOWER(TRIM({{ country_column }})) IN ('taiwan', 'republic of china', 'roc') THEN 'Taiwan'
    
    -- Standard countries (no change needed, just normalize case)
    WHEN LOWER(TRIM({{ country_column }})) = 'israel' THEN 'Israel'
    WHEN LOWER(TRIM({{ country_column }})) = 'germany' THEN 'Germany'
    WHEN LOWER(TRIM({{ country_column }})) = 'france' THEN 'France'
    WHEN LOWER(TRIM({{ country_column }})) = 'switzerland' THEN 'Switzerland'
    WHEN LOWER(TRIM({{ country_column }})) = 'japan' THEN 'Japan'
    WHEN LOWER(TRIM({{ country_column }})) = 'canada' THEN 'Canada'
    WHEN LOWER(TRIM({{ country_column }})) = 'australia' THEN 'Australia'
    WHEN LOWER(TRIM({{ country_column }})) = 'india' THEN 'India'
    WHEN LOWER(TRIM({{ country_column }})) = 'netherlands' THEN 'Netherlands'
    WHEN LOWER(TRIM({{ country_column }})) = 'spain' THEN 'Spain'
    WHEN LOWER(TRIM({{ country_column }})) = 'italy' THEN 'Italy'
    WHEN LOWER(TRIM({{ country_column }})) = 'sweden' THEN 'Sweden'
    WHEN LOWER(TRIM({{ country_column }})) = 'belgium' THEN 'Belgium'
    WHEN LOWER(TRIM({{ country_column }})) = 'austria' THEN 'Austria'
    WHEN LOWER(TRIM({{ country_column }})) = 'denmark' THEN 'Denmark'
    WHEN LOWER(TRIM({{ country_column }})) = 'norway' THEN 'Norway'
    WHEN LOWER(TRIM({{ country_column }})) = 'finland' THEN 'Finland'
    WHEN LOWER(TRIM({{ country_column }})) = 'poland' THEN 'Poland'
    WHEN LOWER(TRIM({{ country_column }})) = 'portugal' THEN 'Portugal'
    WHEN LOWER(TRIM({{ country_column }})) = 'greece' THEN 'Greece'
    WHEN LOWER(TRIM({{ country_column }})) = 'hungary' THEN 'Hungary'
    WHEN LOWER(TRIM({{ country_column }})) = 'romania' THEN 'Romania'
    WHEN LOWER(TRIM({{ country_column }})) = 'bulgaria' THEN 'Bulgaria'
    WHEN LOWER(TRIM({{ country_column }})) = 'croatia' THEN 'Croatia'
    WHEN LOWER(TRIM({{ country_column }})) = 'slovakia' THEN 'Slovakia'
    WHEN LOWER(TRIM({{ country_column }})) = 'slovenia' THEN 'Slovenia'
    WHEN LOWER(TRIM({{ country_column }})) = 'estonia' THEN 'Estonia'
    WHEN LOWER(TRIM({{ country_column }})) = 'latvia' THEN 'Latvia'
    WHEN LOWER(TRIM({{ country_column }})) = 'lithuania' THEN 'Lithuania'
    WHEN LOWER(TRIM({{ country_column }})) = 'ukraine' THEN 'Ukraine'
    WHEN LOWER(TRIM({{ country_column }})) = 'turkey' THEN 'Turkey'
    WHEN LOWER(TRIM({{ country_column }})) = 'vietnam' THEN 'Vietnam'
    WHEN LOWER(TRIM({{ country_column }})) = 'thailand' THEN 'Thailand'
    WHEN LOWER(TRIM({{ country_column }})) = 'indonesia' THEN 'Indonesia'
    WHEN LOWER(TRIM({{ country_column }})) = 'malaysia' THEN 'Malaysia'
    WHEN LOWER(TRIM({{ country_column }})) = 'philippines' THEN 'Philippines'
    WHEN LOWER(TRIM({{ country_column }})) = 'pakistan' THEN 'Pakistan'
    WHEN LOWER(TRIM({{ country_column }})) = 'bangladesh' THEN 'Bangladesh'
    WHEN LOWER(TRIM({{ country_column }})) = 'sri lanka' THEN 'Sri Lanka'
    WHEN LOWER(TRIM({{ country_column }})) = 'brazil' THEN 'Brazil'
    WHEN LOWER(TRIM({{ country_column }})) = 'argentina' THEN 'Argentina'
    WHEN LOWER(TRIM({{ country_column }})) = 'chile' THEN 'Chile'
    WHEN LOWER(TRIM({{ country_column }})) = 'colombia' THEN 'Colombia'
    WHEN LOWER(TRIM({{ country_column }})) = 'mexico' THEN 'Mexico'
    WHEN LOWER(TRIM({{ country_column }})) = 'peru' THEN 'Peru'
    WHEN LOWER(TRIM({{ country_column }})) = 'panama' THEN 'Panama'
    WHEN LOWER(TRIM({{ country_column }})) = 'guatemala' THEN 'Guatemala'
    WHEN LOWER(TRIM({{ country_column }})) = 'honduras' THEN 'Honduras'
    WHEN LOWER(TRIM({{ country_column }})) = 'jamaica' THEN 'Jamaica'
    WHEN LOWER(TRIM({{ country_column }})) = 'barbados' THEN 'Barbados'
    WHEN LOWER(TRIM({{ country_column }})) = 'south africa' THEN 'South Africa'
    WHEN LOWER(TRIM({{ country_column }})) = 'nigeria' THEN 'Nigeria'
    WHEN LOWER(TRIM({{ country_column }})) = 'kenya' THEN 'Kenya'
    WHEN LOWER(TRIM({{ country_column }})) = 'ghana' THEN 'Ghana'
    WHEN LOWER(TRIM({{ country_column }})) = 'egypt' THEN 'Egypt'
    WHEN LOWER(TRIM({{ country_column }})) = 'morocco' THEN 'Morocco'
    WHEN LOWER(TRIM({{ country_column }})) = 'algeria' THEN 'Algeria'
    WHEN LOWER(TRIM({{ country_column }})) = 'malawi' THEN 'Malawi'
    WHEN LOWER(TRIM({{ country_column }})) = 'mozambique' THEN 'Mozambique'
    WHEN LOWER(TRIM({{ country_column }})) = 'saudi arabia' THEN 'Saudi Arabia'
    WHEN LOWER(TRIM({{ country_column }})) = 'qatar' THEN 'Qatar'
    WHEN LOWER(TRIM({{ country_column }})) = 'bahrain' THEN 'Bahrain'
    WHEN LOWER(TRIM({{ country_column }})) = 'jordan' THEN 'Jordan'
    WHEN LOWER(TRIM({{ country_column }})) = 'lebanon' THEN 'Lebanon'
    WHEN LOWER(TRIM({{ country_column }})) = 'iran' THEN 'Iran'
    WHEN LOWER(TRIM({{ country_column }})) = 'palestine' THEN 'Palestine'
    WHEN LOWER(TRIM({{ country_column }})) = 'luxembourg' THEN 'Luxembourg'
    WHEN LOWER(TRIM({{ country_column }})) = 'malta' THEN 'Malta'
    WHEN LOWER(TRIM({{ country_column }})) = 'cyprus' THEN 'Cyprus'
    WHEN LOWER(TRIM({{ country_column }})) = 'monaco' THEN 'Monaco'
    WHEN LOWER(TRIM({{ country_column }})) = 'liechtenstein' THEN 'Liechtenstein'
    WHEN LOWER(TRIM({{ country_column }})) = 'andorra' THEN 'Andorra'
    WHEN LOWER(TRIM({{ country_column }})) = 'gibraltar' THEN 'Gibraltar'
    WHEN LOWER(TRIM({{ country_column }})) = 'isle of man' THEN 'Isle of Man'
    WHEN LOWER(TRIM({{ country_column }})) = 'new zealand' THEN 'New Zealand'
    WHEN LOWER(TRIM({{ country_column }})) = 'cayman islands' THEN 'Cayman Islands'
    WHEN LOWER(TRIM({{ country_column }})) = 'british virgin islands' THEN 'British Virgin Islands'
    WHEN LOWER(TRIM({{ country_column }})) = 'mauritius' THEN 'Mauritius'
    WHEN LOWER(TRIM({{ country_column }})) = 'georgia' THEN 'Georgia'
    WHEN LOWER(TRIM({{ country_column }})) = 'azerbaijan' THEN 'Azerbaijan'
    WHEN LOWER(TRIM({{ country_column }})) = 'kazakhstan' THEN 'Kazakhstan'
    WHEN LOWER(TRIM({{ country_column }})) = 'afghanistan' THEN 'Afghanistan'
    WHEN LOWER(TRIM({{ country_column }})) = 'albania' THEN 'Albania'
    
    -- NULL or empty handling
    WHEN {{ country_column }} IS NULL OR TRIM({{ country_column }}) = '' OR LOWER(TRIM({{ country_column }})) = 'none' THEN NULL
    
    -- Default: return trimmed original value with proper case
    ELSE INITCAP(TRIM({{ country_column }}))
END
{% endmacro %}


{% macro country_iso_code(country_column) %}
{#
    Returns ISO 3166-1 alpha-2 country code.
    
    Usage:
        {{ country_iso_code('entity_country') }} as country_iso
#}
CASE
    WHEN LOWER(TRIM({{ country_column }})) IN ('united states', 'us', 'usa', 'u.s.', 'u.s.a.', 'america', 'kentucky') THEN 'US'
    WHEN LOWER(TRIM({{ country_column }})) IN ('united kingdom', 'uk', 'u.k.', 'great britain', 'britain', 'england') THEN 'GB'
    WHEN LOWER(TRIM({{ country_column }})) IN ('china', 'caina', 'prc') THEN 'CN'
    WHEN LOWER(TRIM({{ country_column }})) IN ('russia', 'russian federation', 'moscow') THEN 'RU'
    WHEN LOWER(TRIM({{ country_column }})) IN ('south korea', 'korea', 'republic of korea', 'rok') THEN 'KR'
    WHEN LOWER(TRIM({{ country_column }})) IN ('ireland', 'republic of ireland', 'eire') THEN 'IE'
    WHEN LOWER(TRIM({{ country_column }})) IN ('czech republic', 'czechia', 'czech') THEN 'CZ'
    WHEN LOWER(TRIM({{ country_column }})) IN ('iceland', 'island') THEN 'IS'
    WHEN LOWER(TRIM({{ country_column }})) IN ('swaziland', 'eswatini') THEN 'SZ'
    WHEN LOWER(TRIM({{ country_column }})) IN ('eu, brussels', 'eu brussels', 'brussels', 'belgium') THEN 'BE'
    WHEN LOWER(TRIM({{ country_column }})) = 'singapore' THEN 'SG'
    WHEN LOWER(TRIM({{ country_column }})) IN ('united arab emirates', 'uae', 'u.a.e.', 'dubai', 'abu dhabi') THEN 'AE'
    WHEN LOWER(TRIM({{ country_column }})) IN ('hong kong', 'hongkong', 'hk') THEN 'HK'
    WHEN LOWER(TRIM({{ country_column }})) IN ('taiwan', 'republic of china', 'roc') THEN 'TW'
    WHEN LOWER(TRIM({{ country_column }})) = 'israel' THEN 'IL'
    WHEN LOWER(TRIM({{ country_column }})) = 'germany' THEN 'DE'
    WHEN LOWER(TRIM({{ country_column }})) = 'france' THEN 'FR'
    WHEN LOWER(TRIM({{ country_column }})) = 'switzerland' THEN 'CH'
    WHEN LOWER(TRIM({{ country_column }})) = 'japan' THEN 'JP'
    WHEN LOWER(TRIM({{ country_column }})) = 'canada' THEN 'CA'
    WHEN LOWER(TRIM({{ country_column }})) = 'australia' THEN 'AU'
    WHEN LOWER(TRIM({{ country_column }})) = 'india' THEN 'IN'
    WHEN LOWER(TRIM({{ country_column }})) = 'netherlands' THEN 'NL'
    WHEN LOWER(TRIM({{ country_column }})) = 'spain' THEN 'ES'
    WHEN LOWER(TRIM({{ country_column }})) = 'italy' THEN 'IT'
    WHEN LOWER(TRIM({{ country_column }})) = 'sweden' THEN 'SE'
    WHEN LOWER(TRIM({{ country_column }})) = 'austria' THEN 'AT'
    WHEN LOWER(TRIM({{ country_column }})) = 'denmark' THEN 'DK'
    WHEN LOWER(TRIM({{ country_column }})) = 'norway' THEN 'NO'
    WHEN LOWER(TRIM({{ country_column }})) = 'finland' THEN 'FI'
    WHEN LOWER(TRIM({{ country_column }})) = 'poland' THEN 'PL'
    WHEN LOWER(TRIM({{ country_column }})) = 'portugal' THEN 'PT'
    WHEN LOWER(TRIM({{ country_column }})) = 'greece' THEN 'GR'
    WHEN LOWER(TRIM({{ country_column }})) = 'hungary' THEN 'HU'
    WHEN LOWER(TRIM({{ country_column }})) = 'romania' THEN 'RO'
    WHEN LOWER(TRIM({{ country_column }})) = 'bulgaria' THEN 'BG'
    WHEN LOWER(TRIM({{ country_column }})) = 'croatia' THEN 'HR'
    WHEN LOWER(TRIM({{ country_column }})) = 'slovakia' THEN 'SK'
    WHEN LOWER(TRIM({{ country_column }})) = 'slovenia' THEN 'SI'
    WHEN LOWER(TRIM({{ country_column }})) = 'estonia' THEN 'EE'
    WHEN LOWER(TRIM({{ country_column }})) = 'latvia' THEN 'LV'
    WHEN LOWER(TRIM({{ country_column }})) = 'lithuania' THEN 'LT'
    WHEN LOWER(TRIM({{ country_column }})) = 'ukraine' THEN 'UA'
    WHEN LOWER(TRIM({{ country_column }})) = 'turkey' THEN 'TR'
    WHEN LOWER(TRIM({{ country_column }})) = 'vietnam' THEN 'VN'
    WHEN LOWER(TRIM({{ country_column }})) = 'thailand' THEN 'TH'
    WHEN LOWER(TRIM({{ country_column }})) = 'indonesia' THEN 'ID'
    WHEN LOWER(TRIM({{ country_column }})) = 'malaysia' THEN 'MY'
    WHEN LOWER(TRIM({{ country_column }})) = 'philippines' THEN 'PH'
    WHEN LOWER(TRIM({{ country_column }})) = 'pakistan' THEN 'PK'
    WHEN LOWER(TRIM({{ country_column }})) = 'bangladesh' THEN 'BD'
    WHEN LOWER(TRIM({{ country_column }})) = 'sri lanka' THEN 'LK'
    WHEN LOWER(TRIM({{ country_column }})) = 'brazil' THEN 'BR'
    WHEN LOWER(TRIM({{ country_column }})) = 'argentina' THEN 'AR'
    WHEN LOWER(TRIM({{ country_column }})) = 'chile' THEN 'CL'
    WHEN LOWER(TRIM({{ country_column }})) = 'colombia' THEN 'CO'
    WHEN LOWER(TRIM({{ country_column }})) = 'mexico' THEN 'MX'
    WHEN LOWER(TRIM({{ country_column }})) = 'peru' THEN 'PE'
    WHEN LOWER(TRIM({{ country_column }})) = 'south africa' THEN 'ZA'
    WHEN LOWER(TRIM({{ country_column }})) = 'nigeria' THEN 'NG'
    WHEN LOWER(TRIM({{ country_column }})) = 'kenya' THEN 'KE'
    WHEN LOWER(TRIM({{ country_column }})) = 'egypt' THEN 'EG'
    WHEN LOWER(TRIM({{ country_column }})) = 'saudi arabia' THEN 'SA'
    WHEN LOWER(TRIM({{ country_column }})) = 'qatar' THEN 'QA'
    WHEN LOWER(TRIM({{ country_column }})) = 'bahrain' THEN 'BH'
    WHEN LOWER(TRIM({{ country_column }})) = 'jordan' THEN 'JO'
    WHEN LOWER(TRIM({{ country_column }})) = 'lebanon' THEN 'LB'
    WHEN LOWER(TRIM({{ country_column }})) = 'iran' THEN 'IR'
    WHEN LOWER(TRIM({{ country_column }})) = 'palestine' THEN 'PS'
    WHEN LOWER(TRIM({{ country_column }})) = 'luxembourg' THEN 'LU'
    WHEN LOWER(TRIM({{ country_column }})) = 'malta' THEN 'MT'
    WHEN LOWER(TRIM({{ country_column }})) = 'cyprus' THEN 'CY'
    WHEN LOWER(TRIM({{ country_column }})) = 'new zealand' THEN 'NZ'
    WHEN LOWER(TRIM({{ country_column }})) = 'georgia' THEN 'GE'
    
    -- Regions and invalid values
    WHEN LOWER(TRIM({{ country_column }})) IN ('americas', 'north america', 'south america', 'europe', 'asia', 'global', 'oceania', 'eastern europe', 'western europe', 'northern europe', 'southern africa', 'eu') THEN NULL
    WHEN {{ country_column }} IS NULL OR TRIM({{ country_column }}) = '' OR LOWER(TRIM({{ country_column }})) = 'none' THEN NULL
    
    ELSE NULL
END
{% endmacro %}


{% macro is_valid_country(country_column) %}
{#
    Returns TRUE if the value is a valid country, FALSE if it's a region or invalid.
    
    Usage:
        {{ is_valid_country('entity_country') }} as is_valid_country
#}
CASE
    -- Regions - not valid countries
    WHEN LOWER(TRIM({{ country_column }})) IN (
        'americas', 
        'north america', 
        'south america', 
        'europe', 
        'asia', 
        'global', 
        'oceania', 
        'eastern europe', 
        'western europe', 
        'northern europe', 
        'southern africa',
        'eu'
    ) THEN 0
    
    -- NULL or empty - not valid
    WHEN {{ country_column }} IS NULL OR TRIM({{ country_column }}) = '' OR LOWER(TRIM({{ country_column }})) = 'none' THEN 0
    
    -- Everything else is considered a valid country
    ELSE 1
END
{% endmacro %}