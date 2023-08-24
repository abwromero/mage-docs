{% snapshot scd_int_vehicles_cleaned %}

{{
    config(
        target_schema='dev',
        unique_key='vehicle_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('int_src_vehicles_cleaned') }}

{% endsnapshot %}