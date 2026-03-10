{{ config(
    materialized='incremental',
    unique_key='application_number'
) }}

SELECT
    a.application_number,
    a.sponsor_name,
    s.submission_type,
    s.submission_status,
    s.submission_status_date
FROM {{ ref('stg_drug_applications') }} a
JOIN {{ ref('stg_drug_submissions') }} s
    ON a.application_number = s.application_number
WHERE s.submission_type = 'ORIG'
    AND s.submission_status = 'AP'

{% if is_incremental() %}
    AND s.submission_status_date > (SELECT MAX(submission_status_date) FROM {{ this }})
{% endif %}