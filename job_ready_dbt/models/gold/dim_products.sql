with
    snapshot as (select * from {{ ref('scd_products') }}),
    final as (
        select
            -- generate a surrogate key for this SCD2 table using product_id and
            -- dbt_valid_from
            {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }}
            as product_key,

            product_id,
            product_name,
            category,
            price,
            updated_at,
            -- expose the dbt_valid_from and dbt_valid_to columns for downstream use
            dbt_valid_from,
            dbt_valid_to
        from snapshot
    )
select *
from final
