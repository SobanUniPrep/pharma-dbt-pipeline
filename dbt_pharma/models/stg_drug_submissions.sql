WITH source AS (
    SELECT data
    FROM {{ source('raw', 'DRUG_APPLICATIONS') }}
),

flattened AS (
    SELECT 
        data:application_number::STRING AS application_number,
        p.value AS submission_data
    FROM source,
    LATERAL FLATTEN(input => data:submissions) p
)

SELECT 
    application_number,
    submission_data:submission_type::STRING AS submission_type,
    submission_data:submission_status::STRING AS submission_status,
    submission_data:submission_status_date::STRING AS submission_status_date,
    submission_data:submission_number::STRING AS submission_number
FROM flattened