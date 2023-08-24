WITH
staging_values AS (
    SELECT
        *
    FROM
        {{ ref('stg_src__values') }}
),

condensed_sales_table AS (
    SELECT
        transaction_id,
        purchase_date,
        {{ dbt_utils.generate_surrogate_key(['customer_name', 'drivers_license', 'mobile_number', 'company', 'customer_email', 'customer_street', 'customer_city', 'customer_province']) }} AS customer_id,
        {{ dbt_utils.generate_surrogate_key(['vehicle', 'price', 'transmission']) }} AS vehicle_id,
        color,
        bank,
        terms_months
    FROM staging_values
)

SELECT
    *
FROM
    condensed_sales_table
ORDER BY
    transaction_id