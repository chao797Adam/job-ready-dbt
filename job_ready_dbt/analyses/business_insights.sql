-- ==========================================================
-- Business Insights & Verification Queries
-- ==========================================================
-- 1. Top Products by Revenue & Units Sold (Product-level performance)
select
    product_id,
    product_name,
    sum(line_total) as total_revenue,
    sum(quantity) as total_units
from {{ ref('fct_order_items') }}
group by product_id, product_name
order by total_revenue desc
limit 10
;

-- 2. Category-level Revenue Breakdown (Category-level performance)
select category, sum(line_total) as total_revenue, sum(quantity) as total_units
from {{ ref('fct_order_items') }}
group by category
order by total_revenue desc
;
