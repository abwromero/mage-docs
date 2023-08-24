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
    condensed_data.purchase_date AS purchase_date,
    customer_data.first_name AS first_name,
    customer_data.last_name AS last_name,
    customer_data.mobile_number AS mobile_number,
    customer_data.customer_email AS customer_email,
    customer_data.customer_province AS customer_province,
    vehicle_data.brand_name AS brand_name,
    vehicle_data.model_name AS model_name,
    vehicle_data.transmission AS transmission
FROM condensed_data
JOIN customer_data ON condensed_data.customer_id = customer_data.customer_id
JOIN vehicle_data ON condensed_data.vehicle_id = vehicle_data.vehicle_id
