SELECT
    p.dosage_form,
    p.marketing_status,
    msc.category,
    COUNT(*) AS total_products
FROM {{ ref('stg_drug_products') }} AS p
JOIN {{ ref('marketing_status_categories') }} AS msc
    ON p.marketing_status = msc.marketing_status
GROUP BY p.dosage_form, p.marketing_status, msc.category
ORDER BY total_products DESC