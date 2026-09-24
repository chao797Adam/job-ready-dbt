{{
    config(
        materialized='incremental',
        unique_key=['order_item_key'],
        merge_update_columns=['quantity', 'unit_price', 'line_total', 'updated_at']
    )
}}

-- Fact: one row per order item (incremental)
with
    order_items as (
        select *
        from {{ ref('int_order_items_with_products') }}
        {% if is_incremental() %}
            where updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}
            as order_item_key,
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            line_total,
            created_at,
            updated_at,
            product_name,
            category,
            product_price,
            customer_id,
            order_date,
            status,
            order_total_amount
        from order_items
        qualify
            row_number() over (
                partition by order_id, product_id order by updated_at desc
            )
            = 1
    )

select *
from final
