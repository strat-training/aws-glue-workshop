# Apache Iceberg Bronze-to-Silver Architecture

## Storage: Apache Iceberg on S3

The pipeline uses Apache Iceberg tables managed by the AWS Glue Data Catalog (`org.apache.iceberg.aws.glue.GlueCatalog`). Iceberg provides ACID transactions, SQL `MERGE INTO`, schema evolution, and time travel for data stored in Amazon S3.

## Idempotent upserts

Silver dimensions and facts use `MERGE INTO`:

- Dimensions match on SHA-256 surrogate keys (`branch_id`, `product_id`).
- Facts match on `order_id` and update existing metrics on re-ingestion.
- The demo treats each source row as one complete order. For order-line data, use a stable `order_line_id` instead.

## Configuration-driven processing

Schema definitions, data-quality assertions, approved products and branches, and output field selections are externalized in `config/sales_config.json`. The job reads this file from S3 at runtime, so business rules can change without changing the ETL engine.

## Parameters and bookmark context

The job accepts `--S3_BUCKET`, `--DATABASE_NAME`, `--TARGET_DATABASE`, `--TABLE_NAME`, and `--CONFIG_KEY` as Glue arguments. It constructs the read context dynamically as `f"{DATABASE_NAME}_{TABLE_NAME}_incremental_ctx"`.

## Surrogate keys

Dimension keys are SHA-256 hashes of natural keys such as `city | manager` and `product_name`. The hash is deterministic and independent of row order.

## Quarantine

Invalid rows are appended as Parquet to `s3://<bucket>/04_quarantine/invalid_<table_name>/`. Valid dimensions and facts are merged into Iceberg tables under the Silver warehouse path. The fact table is partitioned by `year` and `month`.
