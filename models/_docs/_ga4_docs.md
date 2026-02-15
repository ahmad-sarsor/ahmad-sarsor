{% docs event_id %}
Unique ID for a GA4 event.  
Computed property based on: event_timestamp, event_name, user_id, user_pseudo_id, event_bundle_sequence_id, batch_event_index  
Used internally for events deduplication
{% enddocs %}

{% docs event_name %}
Name of the event.
{% enddocs %}


{% docs ga_session_id %}
Session ID is a timestamp of when a session began. To analyze different sessions outside of Google Analytics, consider joining the user_id or user_pseudo_id with the session_id to get a unique identifier for each session.

Example value: 1723017756

https://support.google.com/analytics/answer/9191807?sjid=8301834509801400621-EU
{% enddocs %}

{% docs session_id %}
Unique GA4 session ID.  
Computed property: user_pseudo_id + ga_session_id  
Used internally for merging tables
{% enddocs %}

{% docs session_date %}
Date of the session (formatted as 'YYYY-MM-DD').
{% enddocs %}

{% docs session_duration %}
Difference between the session min and max events (in seconds).
{% enddocs %}

{% docs engagement_time_seconds %}
Total engagement time in seconds for the session, aggregated from GA4's `engagement_time_msec` field.
{% enddocs %}

{% docs suspected_fraud %}
Indicates whether the session has been flagged for potential fraudulent activity.  
This is based on the country + date combination as determined in:  
https://docs.google.com/spreadsheets/d/11JjhBSS9VP9gXHp3cFl24i4Gz73o5t3kqaBlxy7KbZ4
{% enddocs %}

{% docs page_path %}
Extracted path from the page_location URL.
{% enddocs %}

{% docs referrer_path %}
Extracted path from the page_referrer URL.
{% enddocs %}

{% docs grouped_referrer_path %}
Normalized version of the referrer path for categorization purposes.
{% enddocs %}

{% docs grouped_referrer_name %}
Friendly name of the referrer page, derived from the grouped path.
{% enddocs %}

{% docs page_query %}
Extracted query parameters string from the page_location URL, Decoded url-encoded values.
{% enddocs %}

{% docs company_url_name %}
URL-friendly identifier for the company.
{% enddocs %}

{% docs grouped_page_path %}
Normalized and categorized base paths, based on page_path.
{% enddocs %}

{% docs grouped_page_name %}
Normalized and categorized path names, based on page_path.
{% enddocs %}

{% docs device__category %}
The device category (mobile, tablet, desktop).
{% enddocs %}

{% docs device__mobile_brand_name %}
The device brand name.
{% enddocs %}

{% docs device__mobile_model_name %}
The device model name.
{% enddocs %}

{% docs device__mobile_marketing_name %}
The device marketing name.
{% enddocs %}

{% docs device__mobile_os_hardware_model %}
The device model information retrieved directly from the operating system.
{% enddocs %}

{% docs device__operating_system %}
The operating system of the device.
{% enddocs %}

{% docs device__operating_system_version %}
The OS version.
{% enddocs %}

{% docs device__language %}
The OS language.
{% enddocs %}

{% docs device__browser %}
The browser in which the user viewed content.
{% enddocs %}

{% docs device__browser_version %}
The version of the browser in which the user viewed content.
{% enddocs %}

{% docs geo__city %}
The city from which events were reported, based on IP address.
{% enddocs %}

{% docs geo__country %}
The country from which events were reported, based on IP address.
{% enddocs %}

{% docs geo__continent %}
The continent from which events were reported, based on IP address.
{% enddocs %}

{% docs geo__region %}
The region from which events were reported, based on IP address.
{% enddocs %}

{% docs geo__sub_continent %}
The subcontinent from which events were reported, based on IP address.
{% enddocs %}

{% docs traffic_source__name %}
Name of the marketing campaign that first acquired the user.
{% enddocs %}

{% docs traffic_source__medium %}
Name of the medium (paid search, organic search, email, etc.) that first acquired the user.
{% enddocs %}

{% docs traffic_source__source %}
Name of the network that first acquired the user.
{% enddocs %}

{% docs page_referrer %}
The referring URL, which is the user's previous URL and can be your website's domain or other domains.
Example value: http://example.com.
{% enddocs %}

{% docs page_location %}
The complete URL of the webpage that someone visited on your website.
For example, if someone visits www.googlemerchandisestore.com/Bags?theme=1, then the complete URL will populate the dimension.
{% enddocs %}

{% docs page_title %}
The HTML page title that you set on your website.
Example value: Home.
{% enddocs %}

{% docs engagement_time_msec %}
The amount of time a user spends with your web page in focus or app screen in the foreground.  
https://support.google.com/analytics/answer/11109416?hl=en
{% enddocs %}

{% docs marketing_source %}
The source from which the user arrived (e.g., google, facebook).  
Cleaned and prioritized traffic source from UTM, event, or collected data; standardized into known platforms or custom labels. 
{% enddocs %}

{% docs marketing_medium %}
Marketing channel medium (e.g., 'email', 'cpc', 'organic') derived from UTM, event parameters, or traffic source.
{% enddocs %}

{% docs marketing_campaign %}
Name of the marketing campaign from UTM or other sources, representing the campaign identity.  
{% enddocs %}

{% docs marketing_campaignid %}
The unique identifier for the marketing campaign  
{% enddocs %}

{% docs marketing_date %}
The date the marketing campaign was active  
{% enddocs %}

{% docs marketing_company %}
The company associated with the marketing campaign  
{% enddocs %}

{% docs marketing_region %}
The targeted region for the marketing campaign  
{% enddocs %}

{% docs marketing_channel %}
The channel used for marketing  
{% enddocs %}

{% docs marketing_details %}
Additional details about the marketing campaign  
{% enddocs %}

{% docs marketing_objective %}
The objective or goal of the marketing campaign  
{% enddocs %}

{% docs marketing_campaign_group %}
The group or category the marketing campaign belongs to  
{% enddocs %}

{% docs channel_grouping %}
A derived field assinging marketing attribution  
{% enddocs %}