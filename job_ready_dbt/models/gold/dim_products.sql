with
    snapshot as (select * from {{ ref('scd_products') }}),
    final as (
        select
            product_id,
            product_name,
            category,
            price,
            updated_at,
            {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }}
            as product_key,  --
            dbt_valid_from,
            dbt_valid_to
        from snapshot
        where dbt_valid_to is null  --
    )
select *
from final
