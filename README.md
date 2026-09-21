# QBS Sales Data Platform

**QuickByte Sales (QBS)** operates a configuration-driven sales data platform built on AWS Glue, Amazon S3, the AWS Glue Data Catalog, AWS Glue Data Quality, Apache Spark, and Apache Iceberg.

This repository contains the implementation and deployment configuration for the QBS Bronze-to-Silver and Silver-to-Gold data pipelines. Resource names and account-specific values are centralized so the platform can be managed consistently across environments.

## Overview

The pipelines ingest raw sales records from Amazon S3, apply schema mapping and data-quality checks, quarantine invalid records, publish query-ready Silver data as Iceberg tables, and create Gold KPI tables for business analysis.

The platform provides:

- Runtime schema and business-rule configuration from `config/sales_config.json`.
- AWS Glue Data Quality rules plus PySpark validation.
- Deduplication of valid records by `order_id`.
- SHA-256 surrogate keys for branch and product dimensions.
- Idempotent Iceberg `MERGE INTO` operations.
- A quarantine area that preserves rejected source values for investigation.
- Fact-table partitioning by order year and month.
- Gold-layer KPI aggregates for branch performance, menu analysis, and channel behavior.

## Repository layout

```text
config/
	sales_config.json                 Runtime schema and data-quality rules
docs/
	architecture.md                   Pipeline design decisions
	config_specification.md           Configuration reference
	glue_job_config.json.template     Glue argument template
	negative_testing.md               Validation test plan
scripts/
	qbs_bronze-to-silver.py           AWS Glue ETL job
	qbs_bronze-to-silver.json         Glue job definition template
	qbs_silver-to-gold.py             Gold KPI aggregation job
	qbs_silver-to-gold.json           Gold Glue job definition template
```

## AWS resources deployed in the console

The QBS resources used by the platform are:

| Resource | Name | Purpose |
|---|---|---|
| S3 bucket | `qbs-workshop-bucket` | Data lake storage for raw, Silver, Gold, configuration, and quarantine data |
| Glue classifier | `qbs_csv_classifier` | Detects and assigns the schema for incoming CSV data |
| Glue database | `qbs_bronze` | Catalog for raw source tables |
| Glue database | `qbs_silver` | Catalog for Silver Iceberg tables |
| Glue database | `qbs_gold` | Catalog for Gold KPI and analytics tables |
| Glue database | `qbs_quarantine` | Catalog for rejected and quarantined records |
| Glue crawler | `qbs_bronze_crawler` | Registers raw sales data in `qbs_bronze` |
| IAM role | `qbs-AWSGlueServiceRoleAccess` | Allows Glue to access S3, Glue Catalog, and CloudWatch |
| Glue job | `qbs_bronze-to-silver` | Validates, transforms, quarantines, and upserts sales data |
| Glue job | `qbs_silver-to-gold` | Builds Gold KPI tables from `qbs_silver.fact_sales` |
| Athena workgroup | `qbs_workgroup` | Provides the governed query environment for analytics |
| Glue visual ETL | `qbs_etl_pipeline` | Visual ETL workflow for managed data preparation |
| Glue crawler | `qbs_quarantine_crawler` | Registers quarantined records for investigation |
| Glue Data Quality ruleset | `qbs_dqdl_ruleset` | Directs failed records to quarantine |

The data lake uses the following S3 prefixes:

| Prefix | Purpose |
|---|---|
| `01_bronze/` | Incoming sales files |
| `02_silver/` | Silver Iceberg warehouse location |
| `03_gold/` | Gold Iceberg warehouse location |
| `04_quarantine/invalid_sales_raw/` | Rejected records in Parquet format |

Replace the bucket, account, region, and IAM role values with those from the deployment account. The job template deliberately uses placeholders for account-specific Glue asset paths.

## Data flow

```text
S3 01_bronze/sales.csv
				|
				v
Glue Catalog: qbs_bronze.sales_raw
				|
				v
qbs_bronze-to-silver.py
	- read runtime configuration
	- cast and normalize columns
	- run DQDL and PySpark checks
	- quarantine invalid records
	- deduplicate valid records
	- calculate metrics and surrogate keys
				|
				+--> S3 04_quarantine/invalid_sales_raw/ (Parquet)
				|
				v
Iceberg warehouse in S3 02_silver/
				|
				+--> qbs_silver.dim_branch
				+--> qbs_silver.dim_product
				+--> qbs_silver.fact_sales
				|
				v
		qbs_silver-to-gold.py
				|
				+--> qbs_gold.branch_performance
				+--> qbs_gold.menu_quadrant
				+--> qbs_gold.menu_rank_volatility
				+--> qbs_gold.channel_mix
				+--> qbs_gold.channel_growth_correlation
```

## Data model

The Silver layer is a compact star schema. Each source row represents one complete order.

### `dim_branch`

| Column | Type | Description |
|---|---|---|
| `branch_id` | string | SHA-256 surrogate key for `city` and `manager` |
| `city` | string | Normalized branch city |
| `manager` | string | Branch manager |
| `created_date` | timestamp | First insertion timestamp |
| `last_updated_date` | timestamp | Latest merge timestamp |

### `dim_product`

| Column | Type | Description |
|---|---|---|
| `product_id` | string | SHA-256 surrogate key for `product_name` |
| `product_name` | string | Approved product name |
| `created_date` | timestamp | First insertion timestamp |
| `last_updated_date` | timestamp | Latest merge timestamp |

### `fact_sales`

| Column | Type | Description |
|---|---|---|
| `branch_id` | string | Foreign key to `dim_branch` |
| `product_id` | string | Foreign key to `dim_product` |
| `order_id` | string | Business key for the order |
| `order_date` | date | Order date |
| `purchase_type` | string | Sales channel or purchase type |
| `payment_method` | string | Payment method |
| `unit_price` | double | Price per unit |
| `quantity` | double | Quantity sold; fractional values are allowed |
| `total_amount` | double | `unit_price * quantity`, rounded to two decimals |
| `is_fractional_qty` | boolean | Indicates a non-integer quantity |
| `month` | integer | Partition month derived from `order_date` |
| `year` | integer | Partition year derived from `order_date` |
| `created_date` | timestamp | First insertion timestamp |
| `last_updated_date` | timestamp | Latest merge timestamp |

The fact table is partitioned by `year` and `month`. Invalid or unapproved records never populate the Silver dimensions or fact table.

## Gold KPI model

The `qbs_silver-to-gold` job reads `qbs_silver.fact_sales` and recreates five Iceberg tables under the `03_gold/` warehouse path. Each table includes an `as_of_date` timestamp.

| Table | Grain | Purpose |
|---|---|---|
| `branch_performance` | Branch and month | Revenue change decomposed into volume and price effects |
| `menu_quadrant` | Branch and product | Classifies products as core, premium/niche, value driver, or underperformer using revenue and volume tiers |
| `menu_rank_volatility` | Product | Measures variation in product revenue rank across branches |
| `channel_mix` | Branch and purchase type | Counts orders, revenue, and share of branch revenue by channel |
| `channel_growth_correlation` | One platform-level row | Correlates online revenue share with average month-over-month branch revenue growth |

The Gold job uses `createOrReplace()` for deterministic full refreshes of these derived analytical tables. Run it after the Bronze-to-Silver job completes successfully.

## Getting started

1. Create the S3 bucket and prefixes listed above.
2. Upload the source sales file under `01_bronze/`.
3. Create the `qbs_bronze` database and `sales_raw` crawler/table in AWS Glue.
4. Upload `config/sales_config.json` to the configured S3 path.
5. Create the `qbs_dqdl_ruleset` ruleset in AWS Glue Data Quality.
6. Create the Glue job using `scripts/qbs_bronze-to-silver.json`, replacing all account-specific placeholders.
7. Create the Glue job using `scripts/qbs_silver-to-gold.json`, replacing the same account-specific placeholders.
8. Run `qbs_bronze-to-silver`, then run `qbs_silver-to-gold`.
9. Inspect the Iceberg tables in `qbs_silver` and `qbs_gold`.
10. Use `docs/negative_testing.md` to verify quarantine and validation behavior.

For an environment-specific deployment, update the configuration first, then align the raw Glue table, DQDL ruleset, job arguments, and resource names.
