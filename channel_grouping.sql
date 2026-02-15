{% macro channel_grouping() %}
CASE
    WHEN marketing_source = '(direct)' AND marketing_medium IN ('not set', 'none') THEN 'Direct'
    WHEN REGEXP_CONTAINS(marketing_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
  AND REGEXP_CONTAINS(marketing_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Search'
    WHEN REGEXP_CONTAINS(marketing_source,'facebook|fb|linkedin|twitter|lin|tw') AND REGEXP_CONTAINS(marketing_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Social'
    WHEN REGEXP_CONTAINS(marketing_source,'youtube|vimeo')
  AND REGEXP_CONTAINS(marketing_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Video'
    WHEN marketing_medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
    WHEN REGEXP_CONTAINS(marketing_source,'facebook|fb|linkedin|twitter|lin|tw|social')
  OR marketing_medium IN ('social',
    'social-network',
    'social-media',
    'sm',
    'social network',
    'social media', 'linkedin') THEN 'Organic Social'
    WHEN REGEXP_CONTAINS(marketing_source,'youtube|vimeo') OR REGEXP_CONTAINS(marketing_medium,'^(.*video.*)$') THEN 'Organic Video'
    WHEN REGEXP_CONTAINS(marketing_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex|goog')
  OR marketing_medium = '(organic)' THEN 'Organic Search'
    WHEN REGEXP_CONTAINS(marketing_source,'email|e-mail|e_mail|e mail') OR REGEXP_CONTAINS(marketing_medium,'email|e-mail|e_mail|e mail') THEN 'Email' 
    -- referral may also includes misleading links FROM social media NOT recognized
    WHEN marketing_medium = '(referral)' THEN 'Referral'
  ELSE
  'Unassigned'
END AS channel_grouping,
{% endmacro %}