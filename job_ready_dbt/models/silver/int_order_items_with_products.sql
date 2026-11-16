with
    order_items as (select * from {{ ref('stg_order_items') }}),
    products as (
        select * from {{ ref('scd_products') }} where dbt_valid_to is null  -- avoid fan-out by only selecting the current version of each product
    ),
    orders as (select * from {{ ref('stg_orders') }}),
    joined as (
        select
            oi.order_item_id,
            oi.order_id,
            oi.product_id,
            oi.quantity,
            oi.unit_price,
            oi.quantity * oi.unit_price as line_total,

            oi.created_at as order_item_created_at,
            oi.updated_at as order_item_updated_at,

            o.customer_id,
            o.order_date,
            o.status,
            o.total_amount as order_total_amount,

            o.created_at as order_created_at,
            o.updated_at as order_updated_at,

            p.product_name,
            p.category,
            p.price as product_price
        from order_items oi
        left join products p on oi.product_id = p.product_id
        left join orders o on oi.order_id = o.order_id
    )
select *
from joined
