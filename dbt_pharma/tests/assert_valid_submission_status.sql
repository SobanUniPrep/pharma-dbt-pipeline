-- Test selže pokud existuje submission s neplatným statusem
SELECT *
FROM {{ ref('stg_drug_submissions') }}
WHERE submission_status NOT IN ('AP', 'TA', 'RF', 'WD')