{% docs event_datetime_utc %}
The date and time (in ISO 8601 format YYYY-MM-DDTHH:mm:ssZ, UTC) when the event was logged on the client.
{% enddocs %}

{% docs event_date %}
Date (UTC) when the page view occurred, derived from the event timestamp.  Formatted as 'YYYY-MM-DD'.
{% enddocs %}

{% docs first_event_datetime_utc %}
The timestamp of the user's first event (in ISO 8601 format YYYY-MM-DDTHH:mm:ssZ, UTC).
{% enddocs %}

{% docs date_day %}
The day of the month for the date, formatted as a numeric day. For example, '07' from '2024-08-07'. This field represents the day portion of a given date in the format 'YYYY-MM-DD'.
{% enddocs %}

{% docs day_of_week %}
The numeric representation of the day of the week, where 1 is Monday and 7 is Sunday. For example, '4' for Thursday.
{% enddocs %}

{% docs day_of_week_name %}
The full name of the day of the week. For example, 'Wednesday'.
{% enddocs %}

{% docs year_week %}
The year and week number in ISO format, represented as 'YYYY-Www'. For example, '2024-W32' for the 32nd week of 2024.
{% enddocs %}

{% docs year_month %}
The year and month in ISO format, represented as 'YYYY-MM'. For example, '2024-M08' for August 2024.
{% enddocs %}

{% docs month_name %}
The full name of the month. For example, 'August'.
{% enddocs %}

{% docs year_quarter %}
The year and quarter in ISO format, represented as 'YYYY-Qq'. For example, '2024-Q3' for the third quarter of 2024.
{% enddocs %}

{% docs year_number %}
The four-digit representation of the year. For example, '2024'.
{% enddocs %}