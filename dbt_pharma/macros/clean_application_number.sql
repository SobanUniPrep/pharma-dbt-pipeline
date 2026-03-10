{% macro clean_application_number(column_name) %}
    UPPER(TRIM({{ column_name }}))
{% endmacro %}