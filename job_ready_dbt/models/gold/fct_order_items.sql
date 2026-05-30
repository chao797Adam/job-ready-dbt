{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'product_id'],
        merge_update_columns=['quantity', 'price', 'line_revenue']
    )
}}

-- Fact: one row per order item (incremental)
with
    order_items as (
        select *
        from {{ ref('int_order_items_with_products') }}  -- 💡 确认一下你的明细中间表是\udc8d是\udc8f这个\udc90\udc8d字
        {% if is_incremental() %}
            where order_date > (select max(order_date) from {{ this }})
        {% endif %}
    ),

    final as (
        select
            -- 1. 代�\udc90�键放最�\udc8d�\udc9d�
            {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}
            as order_item_key,
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            line_total as line_revenue,
            product_name,
            category,
            product_price,
            customer_id,
            order_date,
            status,
            order_total_amount
        from order_items
    )

select *
from final
