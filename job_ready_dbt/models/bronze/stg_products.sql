with source as (select * from {{ ref('raw_products') }})
select product_id, trim(product_name) as product_name, trim(category) as category, price
from source
