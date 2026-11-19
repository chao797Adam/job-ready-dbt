with
    source as (select * from {{ source('raw_ecommerce', 'raw_products') }}),

    cleaned as (
        select
            product_id,
            trim(product_name) as product_name,
            trim(category) as category,
            cast(price as decimal(10, 2)) as price,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at
        from source
    ),

    deduplicated as (
        select *
        from cleaned
        qualify
            row_number() over (
                partition by product_id order by updated_at desc, created_at desc
            )
            = 1
    )

select *
from deduplicated
