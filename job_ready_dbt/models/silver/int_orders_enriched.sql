with
    orders as (select * from {{ ref('stg_orders') }}),
    customers as (select * from {{ ref('scd_customers') }} where dbt_valid_to is null),
    order_items as (
        select
            order_id,
            count(*) as line_items,
            sum(quantity) as total_quantity,
            sum(unit_price * quantity) as total_revenue
        from {{ ref('stg_order_items') }}
        group by order_id
    ),
    joined as (
        select
            o.order_id,
            o.customer_id,
            o.order_date,
            o.status,
            o.total_amount,
            c.first_name,
            c.last_name,
            c.email,
            c.country,
            c.created_at as customer_created_at,
            coalesce(oi.line_items, 0) as line_items,
            coalesce(oi.total_quantity, 0) as total_quantity,
            coalesce(oi.total_revenue, 0) as line_total_amount
        from orders o
        left join customers c on o.customer_id = c.customer_id
        left join order_items oi on o.order_id = oi.order_id
    )
select *
from joined
