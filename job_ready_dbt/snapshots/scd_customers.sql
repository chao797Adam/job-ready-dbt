{% snapshot scd_customers %}
    {{ config(
        target_schema='silver',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'country']
    )}}

    select customer_id, first_name, last_name, email, country, created_at
    from {{ ref('stg_customers') }}
{% endsnapshot %}
