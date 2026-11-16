with source as (select * from {{ source('raw_ecommerce', 'raw_orders') }})
select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    status,
    total_amount,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from source
