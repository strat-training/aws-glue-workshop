# Apache Iceberg Bronze-to-Gold Architecture

## Storage: Apache Iceberg on S3

The pipeline uses Apache Iceberg tables managed by the AWS Glue Data Catalog (`org.apache.iceberg.aws.glue.GlueCatalog`). Iceberg provides ACID transactions, SQL `MERGE INTO`, schema evolution, and time travel for data stored in Amazon S3.

## Bronze-to-Silver processing

Silver dimensions and facts use `MERGE INTO`:

- Dimensions match on SHA-256 surrogate keys (`branch_id`, `product_id`).
- Facts match on `order_id` and update existing metrics on re-ingestion.
- The demo treats each source row as one complete order. For order-line data, use a stable `order_line_id` instead.

The `qbs_bronze-to-silver` job reads `qbs_bronze.sales_raw`, applies the runtime configuration, evaluates Glue Data Quality and PySpark checks, writes invalid rows to quarantine, and creates or updates `qbs_silver.dim_branch`, `qbs_silver.dim_product`, and `qbs_silver.fact_sales`.

## Configuration-driven processing

Schema definitions, data-quality assertions, approved products and branches, and output field selections are externalized in `config/sales_config.json`. The job reads this file from S3 at runtime, so business rules can change without changing the ETL engine.

## Parameters and bookmark context

The job accepts `--S3_BUCKET`, `--DATABASE_NAME`, `--TARGET_DATABASE`, `--TABLE_NAME`, and `--CONFIG_KEY` as Glue arguments. It constructs the read context dynamically as `f"{DATABASE_NAME}_{TABLE_NAME}_incremental_ctx"`.

## Surrogate keys

Dimension keys are SHA-256 hashes of natural keys such as `city | manager` and `product_name`. The hash is deterministic and independent of row order.

## Quarantine

Invalid rows are appended as Parquet to `s3://<bucket>/04_quarantine/invalid_<table_name>/`. Valid dimensions and facts are merged into Iceberg tables under the Silver warehouse path. The fact table is partitioned by `year` and `month`.

## Silver-to-Gold KPI processing

The `qbs_silver-to-gold` job reads `glue_catalog.qbs_silver.fact_sales` and writes derived Iceberg tables to the `03_gold/` warehouse path and the `qbs_gold` Glue database. It accepts the following arguments:

- `--S3_BUCKET`: Root data lake bucket.
- `--DATABASE_NAME`: Source Silver database, normally `qbs_silver`.
- `--TARGET_DATABASE`: Destination Gold database, normally `qbs_gold`.

The job performs a full refresh with Iceberg `createOrReplace()` because all outputs are derived aggregates. It must run after the Bronze-to-Silver job and uses the Silver fact table as its only input.

### Gold outputs

| Table | Grain | Calculation |
|---|---|---|
| `branch_performance` | `branch_id`, month | Month-over-month revenue change split into volume and price effects |
| `menu_quadrant` | `branch_id`, `product_id` | Revenue and volume `ntile(2)` tiers mapped to four product quadrants |
| `menu_rank_volatility` | `product_id` | Standard deviation of product revenue rank across branches |
| `channel_mix` | `branch_id`, `purchase_type` | Orders, revenue, and percentage of branch revenue |
| `channel_growth_correlation` | One aggregate row | Correlation between online revenue share and average branch growth rate |

Every Gold output includes `as_of_date` for freshness tracking. The Gold layer is intended for Athena queries, dashboards, and downstream analytical consumers; it does not replace the conformed Silver fact and dimensions.
