# job_ready_dbt

An industry-standard, analytics-engineering-grade dbt modeling project for an e-commerce / retail domain. It implements modular layered architecture (Bronze/Staging → Intermediate → Marts), incremental processing with anti-fan-out aggregation design, surrogate key governance, and automated data quality tests.

## 📐 Architecture & Lineage

```
raw_* (Seeds / Sources)
  └── stg_* (Staging: Type casting, text cleaning, standardization)
        ├── int_order_items_with_products (Line-item level enrichment)
        └── int_orders_enriched (Order header + SCD current customer + pre-aggregated item metrics)
              ├── dim_products (Product Dimension - Type 1)
              ├── fct_order_items (Incremental Fact - Line item grain)
              └── fct_orders (Incremental Fact - Order grain)
```

## 📁 Project Structure

```
job_ready_dbt/
├── analyses/
├── macros/
├── models/
│   ├── bronze/
│   │   ├── _bronze_models.yml
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_order_items.sql
│   │   └── stg_products.sql
│   ├── silver/ (intermediate)
│   │   ├── int_order_items_with_products.sql
│   │   └── int_orders_enriched.sql
│   └── gold/ (marts)
│       ├── dim_products.sql
│       ├── fct_order_items.sql
│       └── fct_orders.sql
├── snapshots/
├── seeds/
│   ├── raw_customers.csv
│   ├── raw_orders.csv
│   ├── raw_order_items.csv
│   └── raw_products.csv
└── dbt_project.yml
```

## 🧱 Layer-by-Layer Breakdown

### 1. Staging Layer (`models/bronze/`)

**Purpose:** Clean raw seed/source data, standardize text fields (`trim()`, `lower()`, `upper()`), enforce strict casting (`DATE`), and declare schema tests (`unique`, `not_null`, `relationships`).

**Models:**
- `stg_customers`: Cleans string attributes and email formatting.
- `stg_orders`: Casts `order_date` to `DATE`.
- `stg_order_items`: Pure structural passthrough with clean schema.
- `stg_products`: Normalizes product names and categories.

### 2. Intermediate Layer (`models/silver/`)

**Purpose:** Encapsulate complex business logic, pre-aggregate one-to-many relationships before joining to header tables to prevent metric inflation (fan-out trap), and bind active Slowly Changing Dimensions (SCD).

**Models:**
- `int_order_items_with_products`: Joins order items with product catalogs and order metadata, computing line-item totals (`quantity * unit_price`).
- `int_orders_enriched`: Pre-aggregates `stg_order_items` (`line_items`, `total_quantity`, `total_revenue` grouped by `order_id`) prior to joining with `stg_orders` and active SCD customer records (`scd_customers` where `dbt_valid_to is null`).

### 3. Marts Layer (`models/gold/`)

**Purpose:** Analytics-ready fact and dimension models optimized for BI tool consumption (Tableau, Looker, Power BI).

**Models:**
- `dim_products`: Dimension table enriched with deterministic surrogate keys (`product_key`).
- `fct_order_items`: Incremental fact table tracking line-item transactional performance.
- `fct_orders`: Incremental fact table tracking order-level metrics, enriched customer profiles, and aggregate revenue.

## ⚡ Incremental Strategy & Watermarking

Both fact models (`fct_orders`, `fct_order_items`) use incremental materialization with `merge` strategy:

```sql
{% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
{% endif %}
```

- **Unique Keys:** `order_key` (`fct_orders`), `order_item_key` (`fct_order_items`).
- **Merge Update Governance:** Explicitly targets mutating attributes (`merge_update_columns`) to prevent historical data corruption while allowing operational state transitions (status, revenue recalibrations).

## 🛡️ Engineering Best Practices & Trade-offs

- **Anti-Fan-Out Pre-Aggregation:** In `int_orders_enriched`, item metrics are rolled up via `GROUP BY order_id` before joining to orders. Direct joining of 1-to-many child rows to parent headers without pre-aggregation causes metric multiplication/fan-out.
- **Surrogate Key Determinism:** Utilizing `dbt_utils.generate_surrogate_key()` ensures cross-run consistency for surrogate primary/foreign keys.
- **SCD Join Boundary:** Current-state customer attributes are bound via `dbt_valid_to is null` (As-Is representation). Point-in-time (As-Was) financial attribution would require valid-range temporal window joins.
- **Watermark Limitations:** Pure `> max(date)` watermarking can miss same-day late-arriving batches if timestamp granularity is sub-daily; production hardening requires lookback windows or updated-at micro-batching.

## 🚀 Quickstart

```bash
# 1. Load seed data into warehouse
dbt seed

# 2. Run data transformation pipeline (Bronze -> Silver -> Gold)
dbt run

# 3. Execute automated data quality tests
dbt test

# 4. Generate and serve lineage documentation
dbt docs generate
dbt docs serve
```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- ref (https://www.youtube.com/watch?v=tRwIDJvKSEY&t=1425s)