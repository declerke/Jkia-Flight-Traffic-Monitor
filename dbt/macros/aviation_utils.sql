{% macro distance_km(lat1, lon1, lat2, lon2) %}
    round(
        st_distance(
            st_geogpoint({{ lon1 }}, {{ lat1 }}),
            st_geogpoint({{ lon2 }}, {{ lat2 }})
        ) / 1000.0,
        3
    )
{% endmacro %}


{% macro meters_to_feet(meters_col) %}
    round({{ meters_col }} * 3.28084, 0)
{% endmacro %}


{% macro ms_to_knots(ms_col) %}
    round({{ ms_col }} * 1.94384, 1)
{% endmacro %}


{% macro is_peak_hour(hour_col) %}
    ({{ hour_col }} between 4 and 10 or {{ hour_col }} between 14 and 22)
{% endmacro %}