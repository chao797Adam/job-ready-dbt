{{
    config(
        materialized='incremental',
        unique_key='order_key',
        merge_update_columns=['status', 'line_items', 'total_quantity', 'line_total_amount', 'country']  
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
            total_amount,
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
