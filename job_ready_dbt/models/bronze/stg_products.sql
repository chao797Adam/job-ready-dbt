with source as (select * from {{ source('raw_ecommerce', 'raw_products') }})
select product_id, trim(product_name) as product_name, trim(category) as category, price
from source
