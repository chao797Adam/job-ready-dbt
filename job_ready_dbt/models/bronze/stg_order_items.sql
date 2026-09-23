with source as (select * from {{ ref('raw_order_items') }})
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from source
