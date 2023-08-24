{% snapshot scd_int_customers_cleaned %}

{{
    config(
        target_schema='dev',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('int_src_customers_cleaned') }}

{% endsnapshot %}