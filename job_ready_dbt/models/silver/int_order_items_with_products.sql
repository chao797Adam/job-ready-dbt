with
    order_items as (select * from {{ ref('stg_order_items') }}),
    products as (select * from {{ ref('stg_products') }}),
    orders as (select * from {{ ref('stg_orders') }}),
    joined as (
        select
            oi.order_item_id,
            oi.order_id,
            oi.product_id,
            oi.quantity,
            oi.unit_price,
            oi.quantity * oi.unit_price as line_total,
            p.product_name,
            p.category,
            p.price as product_price,
            o.customer_id,
            o.order_date,
            o.status,
            o.total_amount as order_total_amount
        from order_items oi
        left join products p on oi.product_id = p.product_id
        left join orders o on oi.order_id = o.order_id
    )
select *
from joined
