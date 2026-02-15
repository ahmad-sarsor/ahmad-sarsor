{% docs user_id %}
Unique ID for a user (used in MySql and GA4)
{% enddocs %}

{% docs user_pseudo_id %}
Unique ID for a visitor - used in GA4 (Client ID).  
Uniqueness is determind for each desktop / mobile client using a cookie
{% enddocs %}

{% docs is_user_confirmed %}
Indicates whether the user has confirmed their account.  
Based on the confirmed field from Finder database New_User table:  
confirmed is not null and confirmed = 1
{% enddocs %}

{% docs is_user_registered %}
Indicates whether the user has completed registration.  
Based on multiple fields from Finder database New_User table:  
signup_date is not null and (is_password_empty or is_email_validated)
{% enddocs %}

{% docs is_email_validated %}
Indicates whether the user has verified their email address.  
Based on the email_validated field from Finder database New_User table:  
email_validated = 1
{% enddocs %}

{% docs user_first_name %}
The user's first name as provided during account registration or profile update
{% enddocs %}

{% docs user_last_name %}
The user's last name as provided during account registration or profile update
{% enddocs %}

{% docs user_gender %}
The gender of the user, as specified in their profile or registration details
{% enddocs %}

{% docs user_email %}
The primary email address of the user, used for general communication and account login
{% enddocs %}

{% docs is_member %}
Indicates whether the user is a member  
{% enddocs %}

{% docs is_password_empty %}
Indicates whether the user has set a password  
{% enddocs %}

{% docs is_new_user %}
Indicates whether the user is visiting for the first time  
{% enddocs %}

{% docs is_returning_user %}
Indicates whether the user has visited before  
{% enddocs %}

{% docs is_snc_employee %}
Indicates whether the user is an SNC employee.  
Based on the email field from Finder database New_User table:  
email like '%@sncentral.org'  
{% enddocs %}

{% docs snc_region %}
The SNC region associated with the user  
{% enddocs %}

{% docs snc_target %}
The SNC target segment or group  
{% enddocs %}

{% docs israeli %}
Indicates whether the user is from Israel  
{% enddocs %}

{% docs user_primary_usage %}
The primary way the user utilizes the service or product. This could be categorized into different types based on usage patterns
{% enddocs %}

{% docs user_type %}
The type of user based on predefined categories or attributes.  
based on MySQL `category_raw` column
{% enddocs %}

{% docs entity_analysis_type %}
The type of analysis applied to the entity  
{% enddocs %}

{% docs derived_user_type %}
The user type derived from user behavior or attributes  
{% enddocs %}

{% docs is_power_user %}
Indicates whether the user is classified as a power user  
{% enddocs %}

{% docs user_company_id %}
The unique identifier for the user's company  
{% enddocs %}

{% docs user_company_name %}
The name of the user's company  
{% enddocs %}

{% docs user_company_url_name %}
The URL-friendly name of the user's company  
{% enddocs %}

{% docs user_company_type %}
The type of the user's company  
{% enddocs %}

{% docs user_company_subtype %}
The subtype of the user's company  
{% enddocs %}

{% docs user_company_primary_sector %}
The primary sector of the user's company  
{% enddocs %}

{% docs user_company_primary_sector_parent %}
The parent sector of the user's company's primary sector  
{% enddocs %}
