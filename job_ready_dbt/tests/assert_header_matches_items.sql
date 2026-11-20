-- tests/assert_header_matches_items.sql
{{ config(severity = 'warn') }}

select
    order_id,
    total_amount as header_amount,
    line_total_amount as calculated_amount,
    round(total_amount - line_total_amount, 2) as difference
from {{ ref('int_orders_enriched') }}
where abs(total_amount - line_total_amount) > 0.01
