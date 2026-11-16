with source as (select * from {{ source('raw_ecommerce', 'raw_products') }})
select
    product_id,
    trim(product_name) as product_name,
    trim(category) as category,
    cast(price as decimal(10, 2)) as price,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from source
