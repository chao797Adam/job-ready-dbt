{{
    config(
        materialized='incremental',
        unique_key='order_item_key',
        merge_update_columns=['quantity', 'unit_price', 'line_total', 'order_item_updated_at']
    )
}}

-- Fact: one row per order item (incremental)
with
    order_items as (
        select *
        from {{ ref('int_order_items_with_products') }}
        {% if is_incremental() %}
            -- Align with the alias generated in the int layer
            where
                order_item_updated_at
                > (select max(order_item_updated_at) from {{ this }})
        {% endif %}
    ),

    final as (
        select
            -- Line-item primary key: generated using the finest grain
            {{ dbt_utils.generate_surrogate_key(['order_item_id']) }} as order_item_key,

            -- Product foreign key: generated using product_id
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            line_total,
            order_item_created_at,
            order_item_updated_at,
            customer_id,
            order_date,
            status,
            order_total_amount,
            order_created_at,
            order_updated_at,
            product_name,
            category,
            product_price
        from order_items
        -- Defensive deduplication backstop
        qualify
            row_number() over (
                partition by order_item_id order by order_item_updated_at desc
            )
            = 1
    )

select *
from final
