with
    snapshot as (select * from {{ ref('scd_products') }} where dbt_valid_to is null),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

            product_id,
            product_name,
            category,
            price,
            updated_at,
            dbt_valid_from,
            dbt_valid_to
        from snapshot
    )

select *
from final
