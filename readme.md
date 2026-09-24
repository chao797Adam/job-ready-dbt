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

Both fact models (`fct_orders`, `fct_order_items`) use incremental materialization with `merge` strategy, filtering on `updated_at`:

```sql
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

- **Unique Keys:** `order_key` (`fct_orders`), `order_item_key` (`fct_order_items`).
- **Merge Update Governance:** Explicitly targets mutating attributes (`merge_update_columns`) to prevent historical data corruption while allowing operational state transitions (status, revenue recalibrations).

### ✅ Resolved: `order_date` Could Not Serve as a Change-Detection Watermark

**The problem (as originally shipped):** the watermark filtered on `order_date`, which is the date the order was **first created** and never changes afterward. `merge_update_columns` was configured under the assumption that mutable attributes (like `status`) would be re-selected and merged whenever they changed. **These two things were incompatible**: `order_date` can only tell dbt "this is a new order," never "this existing order was updated." `merge_update_columns` silently failed to do its job for any order whose `order_date` fell before the current max — no matter how recently its `status` actually changed.

**Concrete example that exposed the bug** (`raw_orders.csv`, max `order_date` = `2024-07-15`):

| order_id | order_date | status | Re-selected under old `order_date` watermark? | Re-selected under new `updated_at` watermark? |
| --- | --- | --- | --- | --- |
| `ord_018` | 2024-07-12 | `shipped` | ❌ No — `2024-07-12 < 2024-07-15`, filtered out forever | ✅ Yes — `updated_at` (2024-07-20) is newer than any previously loaded `updated_at` |
| `ord_019` | 2024-07-14 | `processing` | ❌ No — same issue | ✅ Yes — `updated_at` (2024-07-22) is newer |

**Why `order_date` fails but `updated_at` works:** `order_date` is written once at creation and frozen forever — it answers "is this a new order?", not "was this row touched?". `updated_at` is (by design) re-stamped every time a row changes, regardless of how old the row's `order_date` is, so it correctly answers the question the watermark actually needs answered.

**Fix applied:** added `created_at` / `updated_at` audit columns to `raw_orders` and `raw_order_items`, threaded them through `stg_orders` → `int_orders_enriched` → `fct_orders` (and the equivalent `order_items` chain), and repointed both incremental filters at `updated_at`. `updated_at` was also added to `merge_update_columns` on both fact models so the row's own audit timestamp gets refreshed on every merge. Verified via `dbt run --full-refresh` after the schema change (required — see note below).

> **Note on rolling out this kind of fix:** `fct_orders`/`fct_order_items` are `incremental` models. Once a table already exists, `dbt run` on an incremental model does a `MERGE` against the existing structure — it does **not** re-run `CREATE TABLE AS SELECT`, so newly added columns like `updated_at` won't appear on their own. Adding columns to an incremental model's output requires `dbt run --full-refresh` (or `--full-refresh --select +fct_orders +fct_order_items` to scope it) to force a full rebuild. `view`/`table` models (staging, intermediate, `dim_products`) don't have this problem — they're dropped and recreated from scratch on every run regardless.

### ✅ Resolved: No Defensive Deduplication Before Merge (test coverage added)

```
unique_key='order_key' is a MATCH key for merge, not a dedup guarantee.
dbt does not dedupe the incoming batch — it assumes upstream already
returns 1 row per key.

Risk: if scd_customers ever returns >1 "current" row per customer_id
(snapshot anomaly), the join fans out → duplicate order_id → duplicate
rows silently inserted into fct_orders (Delta MERGE only blocks
multiple SOURCE rows hitting the same EXISTING target row — it won't
stop duplicates within a fresh insert).
```

**Fix applied:** added `unique` + `not_null` tests on the gold-layer surrogate keys:

```yaml
# models/gold/_gold_models.yml
version: 2

models:
  - name: fct_orders
    columns:
      - name: order_key
        tests: [unique, not_null]

  - name: fct_order_items
    columns:
      - name: order_item_key
        tests: [unique, not_null]

  - name: dim_products
    columns:
      - name: product_key
        tests: [unique, not_null]
```

Verified via `dbt test`: all 6 new tests pass (`unique_fct_orders_order_key`, `unique_fct_order_items_order_item_key`, `unique_dim_products_product_key`, plus their `not_null` counterparts), bringing total project test count from 11 to 17 — `PASS=17 WARN=0 ERROR=0`.

**Fix applied — dedup backstop added:** both fact models now include a `qualify row_number() = 1` step after their `final` CTE's `from`, so a fan-out can no longer produce duplicate rows in the first place (not just get caught by a test afterward):

```sql
-- fct_orders.sql
    final as (
        select ...
        from orders_enriched
        qualify row_number() over (partition by order_id order by updated_at desc) = 1
    )

-- fct_order_items.sql
    final as (
        select ...
        from order_items
        qualify row_number() over (partition by order_id, product_id order by updated_at desc) = 1
    )
```

**Why `qualify`:** equivalent to writing an extra CTE, just without the extra CTE.

```sql
-- Without qualify (2 CTEs needed):
final as (
    select ..., row_number() over (partition by order_id order by updated_at desc) as rn
    from orders_enriched
),
deduped as (
    select * except(rn) from final where rn = 1
)

-- With qualify (1 CTE):
final as (
    select ...
    from orders_enriched
    qualify row_number() over (partition by order_id order by updated_at desc) = 1
)
```

`qualify` must come after `from` (like `having` comes after `group by`) — it filters on the window function's result, which only exists after `from` has been evaluated.

**Note on partition keys:** `fct_order_items` partitions by `order_id, product_id` (not `order_item_id`), because that's what `order_item_key`'s surrogate key is actually generated from — deduping on the wrong grain would let two `order_item_id`s that hash to the same `order_item_key` slip through.

Verified via `dbt run --select fct_orders fct_order_items` (no `--full-refresh` needed — this only adds a filter, no schema change): both models built successfully, `PASS=2 WARN=0 ERROR=0`.

## 🛡️ Engineering Best Practices & Trade-offs

- **Anti-Fan-Out Pre-Aggregation:** In `int_orders_enriched`, item metrics are rolled up via `GROUP BY order_id` before joining to orders. Direct joining of 1-to-many child rows to parent headers without pre-aggregation causes metric multiplication/fan-out.
- **Surrogate Key Determinism:** Utilizing `dbt_utils.generate_surrogate_key()` ensures cross-run consistency for surrogate primary/foreign keys.
- **SCD Join Boundary:** Current-state customer attributes are bound via `dbt_valid_to is null` (As-Is representation). Point-in-time (As-Was) financial attribution would require valid-range temporal window joins.
- **Watermark Limitations:** see [Resolved issue](#-resolved-order_date-could-not-serve-as-a-change-detection-watermark) above — `order_date` watermarking missed *any* status change on an order created before the current max date, not just same-day late arrivals. Fixed by switching to an `updated_at` watermark.
- **Dedup Test Coverage:** see [Resolved issue](#-resolved-no-defensive-deduplication-before-merge-test-coverage-added) above — `unique`/`not_null` tests guard the gold-layer surrogate keys, and a `qualify row_number()` backstop in the model SQL now prevents fan-out duplicates from being written in the first place.

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
      host: <your-workspace>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<your-warehouse-id>
      threads: 4
      token: "Your_TOKEN"
    prod:
      type: databricks
      catalog: job_ready_dbt_prod
      schema: default
      host: <your-workspace>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<your-warehouse-id>
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