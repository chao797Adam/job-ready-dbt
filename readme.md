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

**Why two intermediate models at two different grains:** the business questions this project needs to answer fall into two distinct grains. Questions about *orders as a whole* — revenue per order, average order value, orders per week — need **one row per order** (order grain), which is why `int_orders_enriched` deliberately collapses item-level rows into per-order summary metrics before joining to the order header. Questions about *what was sold* — revenue by product/category, units sold per product — need **one row per order line** (line grain), which is why `int_order_items_with_products` intentionally does *not* aggregate and instead keeps every line item, carrying `product_id` through so it can still be sliced by product. This is a deliberate grain decision, not redundant modeling: it's what produces the two separate fact tables (`fct_orders`, `fct_order_items`) in the marts layer below, each serving the question set that matches its grain.

### 3. Marts Layer (`models/gold/`)

**Purpose:** Analytics-ready fact and dimension models optimized for BI tool consumption (Tableau, Looker, Power BI).

**Models:**

- `dim_products`: Dimension table enriched with deterministic surrogate keys (`product_key`). **Full refresh** (not incremental) — the whole table is rebuilt from `stg_products` on every run; there's no `updated_at`-based change detection here because there's no merge logic to feed.
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

### ⚠️ Known Issue: `order_date` Cannot Serve as a Change-Detection Watermark

The current watermark filters on `order_date`, which is the date the order was **first created** and never changes afterward. `merge_update_columns` is configured under the assumption that mutable attributes (like `status`) will be re-selected and merged whenever they change. **These two things are incompatible**: `order_date` can only tell dbt "this is a new order," never "this existing order was updated." As a result, `merge_update_columns` silently fails to do its job for any order whose `order_date` falls before the current max — no matter how recently its `status` actually changed.

**Concrete example from the seed data** (`raw_orders.csv`, max `order_date` = `2024-07-15`):

| order_id | order_date | status | Re-selected on next incremental run? |
| --- | --- | --- | --- |
| `ord_018` | 2024-07-12 | `shipped` | ❌ No — `2024-07-12 < 2024-07-15`, filtered out forever |
| `ord_019` | 2024-07-14 | `processing` | ❌ No — same issue |

If either order's status later changes to `delivered` in the source system, `fct_orders` will never learn about it. The table will silently drift out of sync with the true order state, with no error raised.

**Root cause:** the seed data has no column that records "when this row was last modified" — only `order_date` (when it was created). Without a true `updated_at` (or equivalent audit) column, incremental change-detection cannot be correctly implemented no matter how the `where` clause is written; this is a data-model gap, not a SQL bug.

**Status:** not yet fixed in this project. Planned remediation is to add `created_at` / `updated_at` audit columns to `raw_orders` and repoint the incremental filter at `updated_at`, documented in a follow-up change.

### ⚠️ Known Issue: No Defensive Deduplication Before Merge

```
unique_key='order_key' is a MATCH key for merge, not a dedup guarantee.
dbt does not dedupe the incoming batch — it assumes upstream already
returns 1 row per key. Nothing enforces that, and no `unique` test
exists on order_key / order_item_key in the gold layer.

Risk: if scd_customers ever returns >1 "current" row per customer_id
(snapshot anomaly), the join fans out → duplicate order_id → duplicate
rows silently inserted into fct_orders (Delta MERGE only blocks
multiple SOURCE rows hitting the same EXISTING target row — it won't
stop duplicates within a fresh insert).

Fix (not yet done): add `unique` tests on both surrogate keys, and
consider a qualify row_number() dedup step as a backstop.
```

## 🛡️ Engineering Best Practices & Trade-offs

- **Anti-Fan-Out Pre-Aggregation:** In `int_orders_enriched`, item metrics are rolled up via `GROUP BY order_id` before joining to orders. Direct joining of 1-to-many child rows to parent headers without pre-aggregation causes metric multiplication/fan-out.
- **Surrogate Key Determinism:** Utilizing `dbt_utils.generate_surrogate_key()` ensures cross-run consistency for surrogate primary/foreign keys.
- **SCD Join Boundary:** Current-state customer attributes are bound via `dbt_valid_to is null` (As-Is representation). Point-in-time (As-Was) financial attribution would require valid-range temporal window joins.
- **Watermark Limitations:** see [Known Issue](#%EF%B8%8F-known-issue-order_date-cannot-serve-as-a-change-detection-watermark) above — `order_date` watermarking misses *any* status change on an order created before the current max date, not just same-day late arrivals. This is more severe than a simple lookback-window gap and requires a true `updated_at` column to fix correctly.
- **No Defensive Dedup on Merge:** see [Known Issue](#%EF%B8%8F-known-issue-no-defensive-deduplication-before-merge) above — the incremental merge trusts upstream models to produce unique keys but never verifies it, and no gold-layer `unique` test exists to catch a violation.

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

## 🚢 Deployment

This project targets **Databricks** and runs against two environments defined as separate targets in `profiles.yml`:

```yaml
job_ready_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: job_ready_dbt
      schema: default
      host: 
      http_path: 
      threads: 4
      token: "Your_TOKEN"
    prod:
      type: databricks
      catalog: job_ready_dbt_prod
      schema: default
      host: 
      http_path: 
      threads: 4
      token: "Your_TOKEN"
```

> ⚠️ **Do not commit your real token.** Replace `"Your_TOKEN"` with your own Databricks personal access token (User Settings → Developer → Access tokens) locally, but never push the real value to git — keep this file out of version control (e.g. via `.gitignore`) or reset `token` back to `"Your_TOKEN"` before committing.

**Run against a specific environment:**

```bash
dbt build --target dev    # seed + run + test against the dev catalog
dbt build --target prod   # same, against the prod catalog
```

## Reference

- [Tutorial video](https://www.youtube.com/watch?v=tRwIDJvKSEY&t=1425s)