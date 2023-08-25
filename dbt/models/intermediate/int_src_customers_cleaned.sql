{{
	config(
		materialized = "table"
	)
}}

WITH
staging_values AS (
    SELECT
        *
    FROM
        {{ ref('stg_src__values') }}
),

removed_titles AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_name', 'drivers_license', 'mobile_number', 'company', 'customer_email', 'customer_street', 'customer_city', 'customer_province']) }} AS customer_id,
        REGEXP_REPLACE(customer_name, 'Mr.|Ms.|Miss|Dr.|Mrs.|Jr.|MD|PhD|DDS|V|DVM', '') AS customer_name_clean,
        drivers_license,
        mobile_number,
        customer_email,
        customer_street,
        customer_city,
        customer_province,
        CURRENT_DATE AS updated_at
    FROM
        staging_values
),

first_and_last_names AS (
    SELECT
        DISTINCT(customer_id) AS customer_id,
        REGEXP_SUBSTR(customer_name_clean, '^\w+') AS first_name,
        REGEXP_SUBSTR(customer_name_clean, '\w+$') AS last_name,
        drivers_license,
        mobile_number,
        customer_email,
        customer_street,
        customer_city,
        customer_province,
        updated_at
    FROM
        removed_titles
)

SELECT
    *
FROM
    first_and_last_names