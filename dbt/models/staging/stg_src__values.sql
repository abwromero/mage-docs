{{
	config(
		materialized = "table"
	)
}}

WITH
stg_values AS (
    SELECT
        *
    FROM
        {{ source('main', 'raw_values') }}
)
SELECT
    id AS transaction_id,
	TO_DATE(
		NULLIF(date, 'null'),
		'YYYY-MM-DD')
		AS purchase_date,
    NULLIF(name, 'null') AS customer_name,
    NULLIF(license, 'null') AS drivers_license,
	CAST(
		REPLACE(   
			REPLACE(
					NULLIF(num, 'null'),
						   '+63',
						   '0'
					),
			'-', ''
			) AS numeric)
			AS mobile_number,
    NULLIF(email, 'null') AS customer_email,
    NULLIF(company, 'null') AS company,
    NULLIF(street, 'null') AS customer_street,
    NULLIF(city, 'null') AS customer_city,
    NULLIF(province, 'null') AS customer_province,
    CASE
        WHEN bank='Asia United Bank' OR bank='Asia United' OR bank='AUB' THEN 'AUB'
        WHEN bank='Banco de Oro' OR bank='BDO' THEN 'BDO'
        WHEN bank='Bank of the Philippine Islands' OR bank='BPI' THEN 'BPI'
        WHEN bank='Chinabank' THEN 'Chinabank'
        WHEN bank='Development Bank of the Philippines' OR bank='Development Bank' OR bank='DBP' THEN 'DBP'
        WHEN bank='In-house' THEN 'In-house'
        WHEN bank='Land Bank of the Philippines' OR bank='Land Bank' OR bank='LBP' THEN 'LBP'
        WHEN bank='Metrobank' OR bank='MB' THEN 'MB'
        WHEN bank='Philippine National Bank' OR bank='PNB' THEN 'PNB'
        WHEN bank='Rizal Commercial Banking Corporation' OR bank='RCBC' THEN 'RCBC'
        WHEN bank='Union Bank of the Philippines' OR bank='UBP' THEN 'UBP'
        ELSE NULL
    END AS bank,
    REGEXP_SUBSTR(car, '.+(?= P[\d\s]+[,.\d]\d+[,\d]\d+)') AS vehicle,
	CAST(REPLACE(
		REGEXP_SUBSTR(car, '(\d+[,\d]\d+[,\d]\d+)'),
		',',
		''
		) AS NUMERIC)AS price,
	REGEXP_SUBSTR(car,'AT|MT') AS transmission,
    NULLIF(plate, 'null') AS plate_number,
    CASE
		WHEN LOWER(color)='sky blue' OR LOWER(color)='skyblue' THEN 'Sky Blue'
		WHEN LOWER(color)='yellow' OR LOWER(color)='ylw' THEN 'Yellow'
		WHEN LOWER(color)='blue' OR LOWER(color)='blu' THEN 'Blue'
		WHEN LOWER(color)='green' THEN 'Green'
		WHEN LOWER(color)='white' THEN 'White'
		WHEN LOWER(color)='teal' THEN 'Teal'
		WHEN LOWER(color)='black' OR LOWER(color)='blk' THEN 'Black'
		WHEN LOWER(color)='red' THEN 'Red'
		WHEN LOWER(color)='silver' THEN 'Silver'
		WHEN LOWER(color)='orange' THEN 'Orange'
		WHEN LOWER(color)='grey' OR LOWER(color)='gray' THEN 'Gray'
        ELSE NULL
	END AS color,
    CASE
		WHEN terms=1 OR terms=12 THEN 12
		WHEN terms=2 OR terms=24 THEN 24
		WHEN terms=3 OR terms=36 THEN 36
		WHEN terms=4 OR terms=48 THEN 48
		WHEN terms=5 OR terms=60 THEN 60
		WHEN terms=6 THEN 72
        ELSE NULL
	END AS terms_months
FROM stg_values
ORDER BY transaction_id