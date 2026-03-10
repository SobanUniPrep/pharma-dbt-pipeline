WITH source AS (
    SELECT data
    FROM {{ source('raw', 'DRUG_APPLICATIONS') }}
)

SELECT 
    data:application_number::STRING AS application_number,
    data:sponsor_name::STRING AS sponsor_name
FROM source