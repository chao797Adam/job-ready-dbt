{{
    config(
        materialized='incremental',
        unique_key='order_id',
        merge_update_columns=['status', 'revenue1', 'number_of_lines', 'total_units', 'customer_country']
    )
}}

-- Fact: one row per order (incremental)
with
    orders_enriched as (
        select *
        from {{ ref('int_orders_enriched') }}
        {% if is_incremental() %}
            where order_date > (select max(order_date) from {{ this }})
        {% endif %}
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,
            order_id,
            customer_id,
            order_date,
            status,
            total_amount as revenue1,
            first_name,
            last_name,
            email,
            country,
            customer_created_at,
            line_items,
            total_quantity,
            line_total_amount
        from orders_enriched
    )

select *
from final
