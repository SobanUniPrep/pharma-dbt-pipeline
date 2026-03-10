SELECT
    sponsor_name,
    COUNT(DISTINCT application_number) AS total_applications
FROM {{ ref('stg_drug_applications') }}
GROUP BY sponsor_name
ORDER BY total_applications DESC