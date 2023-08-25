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

car_details AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['vehicle', 'price', 'transmission']) }} AS vehicle_id,
        vehicle,
        price,
        transmission,
        CURRENT_DATE AS updated_at
    FROM
        staging_values
),

brand_name_initial AS (
    SELECT
        vehicle_id,
        REGEXP_SUBSTR(vehicle, '^\w+') AS brand_name_draft,
        vehicle,
        price,
        transmission,
        updated_at
    FROM
        car_details
),

brand_name_fixed AS (
    SELECT
        vehicle_id,
        CASE
            WHEN brand_name_draft='Land' THEN 'Land Rover'
            WHEN brand_name_draft='Mercedes' THEN 'Mercedes-Benz'
            WHEN brand_name_draft='Great' THEN 'Great Wall'
            ELSE brand_name_draft
        END AS brand_name,
        vehicle,
        price,
        transmission,
        updated_at
    FROM
        brand_name_initial
),

product_names AS (
    SELECT
        DISTINCT(vehicle_id) AS vehicle_id,
        brand_name,
        REPLACE(vehicle, brand_name, '') AS model_name,
        price,
        transmission,
        updated_at
    FROM
        brand_name_fixed
)

SELECT
    *
FROM
    product_names