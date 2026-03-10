WITH source AS (
    SELECT data
    FROM {{ source('raw', 'DRUG_APPLICATIONS') }}
),

flattened AS (
    SELECT 
        data:application_number::STRING AS application_number,
        p.value AS product_data,
        ai.value:name::STRING AS active_ingredient
    FROM source,
    LATERAL FLATTEN(input => data:products) p,
    LATERAL FLATTEN(input => product_data:active_ingredients) ai
)

select 
    application_number,
    active_ingredient,
    product_data:brand_name::STRING AS brand_name,
    product_data:dosage_form::STRING AS dosage_form,
    product_data:route::STRING AS route,
    product_data:marketing_status::STRING AS marketing_status
from flattened