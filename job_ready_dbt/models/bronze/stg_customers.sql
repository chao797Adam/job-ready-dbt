with
    source as (select * from {{ source('raw_ecommerce', 'raw_customers') }}),
    deduplicated as (
        select
            customer_id,
            trim(first_name) as first_name,
            trim(last_name) as last_name,
            lower(trim(email)) as email,
            upper(trim(country)) as country,
            created_at,
            row_number() over (partition by customer_id order by created_at desc) as rn
        from source
    )
select *
from deduplicated
where rn = 1
