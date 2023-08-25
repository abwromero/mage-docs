{{
	config(
		materialized = "table",
        schema= "visualization"
	)
}}

WITH
customer_data AS (
    SELECT * FROM {{ ref('int_src_customers_cleaned') }}
),

vehicle_data AS (
    SELECT * FROM {{ ref('int_src_vehicles_cleaned') }}
),

condensed_data AS(
    SELECT * FROM {{ ref('int_src_sales_condensed') }}
)

SELECT
    condensed_data.purchase_date,
    customer_data.customer_province,
    vehicle_data.brand_name,
    vehicle_data.model_name,
    vehicle_data.transmission,
    condensed_data.color,
    vehicle_data.price,
    condensed_data.bank,
    condensed_data.terms_months
FROM condensed_data
JOIN vehicle_data ON condensed_data.vehicle_id = vehicle_data.vehicle_id
JOIN customer_data ON condensed_data.customer_id = customer_data.customer_id
