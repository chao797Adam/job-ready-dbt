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

-- 3. Average order value per month
select
    date_trunc('month', order_date) as month,
    sum(line_total_amount) as monthly_revenue,
    count(distinct order_id) as monthly_orders,
    sum(line_total_amount) / count(distinct order_id) as avg_order_value
from dbt_job_ready.gold.fct_orders
group by month
order by month
;

-- 4. Which country has the highest revenue
select
    country,
    sum(line_total_amount) as total_revenue,
    count(distinct order_id) as total_orders
from dbt_job_ready.gold.fct_orders
group by 1
order by 2 desc
;

-- 5. Which customer has the highest revenue
select
    o.customer_id,
    max(c.email) as email,
    max(c.country) as country,
    -- order table revenue 1
    sum(line_total_amount) as total_revenue,
    count(o.order_id) as order_count
from dbt_job_ready.gold.fct_orders o
left join
    dbt_job_ready.silver.scd_customers c
    on o.customer_id = c.customer_id
    and c.dbt_valid_to is null
group by o.customer_id
order by total_revenue desc
limit 20
;


-- 6. Which customer has more than one order?
with
    cte as (
        select customer_id, count(order_id) as no_of_order
        from dbt_job_ready.gold.fct_orders
        group by customer_id
        having count(order_id) > 1
    )
select cte.customer_id, cte.no_of_order, c.first_name, c.last_name, c.email, c.country
from cte
left join dbt_job_ready.silver.scd_customers c on cte.customer_id = c.customer_id
where c.dbt_valid_to is null
order by 1
