import sys
import json
import logging
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("Parsing command-line arguments...")
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET", "DATABASE_NAME", "TARGET_DATABASE", "TABLE_NAME", "CONFIG_KEY"]
)

BUCKET = args["S3_BUCKET"]
DATABASE_NAME = args["DATABASE_NAME"]
TARGET_DATABASE = args["TARGET_DATABASE"]
TABLE_NAME = args["TABLE_NAME"]
CONFIG_KEY = args["CONFIG_KEY"]

QUARANTINE_PATH = f"{BUCKET}/04_quarantine/invalid_{TABLE_NAME}/"
WAREHOUSE_PATH = f"{BUCKET}/02_silver/"

logger.info(f"Source Database: {DATABASE_NAME} | Target Database: {TARGET_DATABASE} | Table: {TABLE_NAME}")
logger.info(f"Warehouse Path: {WAREHOUSE_PATH}")
logger.info(f"Quarantine Path: {QUARANTINE_PATH}")

logger.info("Initializing Spark Session with Iceberg support...")
sc = SparkContext()
glueContext = GlueContext(sc)

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE_PATH)
    .getOrCreate()
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def generate_surrogate_key(cols):
    safe_cols = [F.coalesce(F.col(c).cast("string"), F.lit("unknown")) for c in cols]
    return F.sha2(F.concat_ws("|", *safe_cols), 256)


def create_dimension(df, natural_keys, surrogate_key_name, current_ts):
    return (
        df.select(*natural_keys)
        .dropDuplicates()
        .withColumn(surrogate_key_name, generate_surrogate_key(natural_keys))
        .withColumn("created_date", current_ts)
        .withColumn("last_updated_date", current_ts)
    )


def get_catalog_ruleset(ruleset_name):
    try:
        glue_client = boto3.client("glue")
        response = glue_client.get_data_quality_ruleset(Name=ruleset_name)
        return response["Ruleset"]
    except Exception:
        logger.exception("Error retrieving DQDL ruleset '%s'.", ruleset_name)
        raise


logger.info(f"Fetching configuration from S3: {BUCKET}/{CONFIG_KEY}")
try:
    bucket_name = BUCKET.replace("s3://", "").rstrip("/")
    s3_client = boto3.client("s3")
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=CONFIG_KEY)
    TABLE_CONFIG = json.loads(s3_object["Body"].read().decode("utf-8"))
except Exception as error:
    logger.error(f"Error fetching configuration from S3: {error}")
    raise


dynamic_ctx = f"{DATABASE_NAME}_{TABLE_NAME}_incremental_ctx"
logger.info(f"Reading catalog data with Bookmark Context: {dynamic_ctx}")

dyf_raw = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=TABLE_NAME,
    transformation_ctx=dynamic_ctx
)
df_parsed = dyf_raw.toDF()

if df_parsed.rdd.isEmpty():
    logger.warning(f"No new files arrived for table {TABLE_NAME}. Exiting job early.")
    job.commit()
    sys.exit(0)

total_raw_records = df_parsed.count()
logger.info(f"Ingested {total_raw_records} raw records from S3.")

cols_to_drop = []
logger.info("Applying dynamic column casting and renaming...")
for raw_col, rules in TABLE_CONFIG["columns"].items():
    target_col = rules["target_name"]
    col_type = rules["type"]

    if col_type == "integer":
        df_parsed = df_parsed.withColumn(target_col, F.col(raw_col).cast(IntegerType()))
    elif col_type == "double":
        df_parsed = df_parsed.withColumn(target_col, F.col(raw_col).cast(DoubleType()))
    elif col_type == "date":
        formats = rules["format"]
        if isinstance(formats, list):
            date_exprs = [F.to_date(F.col(raw_col), fmt) for fmt in formats]
            df_parsed = df_parsed.withColumn(target_col, F.coalesce(*date_exprs))
        else:
            df_parsed = df_parsed.withColumn(target_col, F.to_date(F.col(raw_col), formats))
    elif col_type == "string":
        df_parsed = df_parsed.withColumn(
            target_col,
            F.regexp_replace(F.trim(F.col(raw_col)), r"\s+", " ")
        )

    if raw_col != target_col:
        cols_to_drop.append(raw_col)

logger.info("Evaluating Data Quality using Glue DQDL and PySpark checks...")
ruleset_name = TABLE_CONFIG.get("dqdl_ruleset_name", "qbs_dqdl_ruleset")
dqdl_string = get_catalog_ruleset(ruleset_name)

dq_frame = DynamicFrame.fromDF(df_parsed, glueContext, "dq_frame_ctx")
dq_results = EvaluateDataQuality().process_rows(
    frame=dq_frame,
    ruleset=dqdl_string,
    publishing_options={
        "dataQualityEvaluationContext": ruleset_name,
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "performanceTuning.caching": "CACHE_NOTHING",
        "observations.scope": "ALL",
    },
)

df_dq = dq_results["rowLevelOutcomes"].toDF()
required_columns = set(TABLE_CONFIG["data_quality"]["not_null"]) | set(
    TABLE_CONFIG["data_quality"]["greater_than_zero"]
)
missing_columns = sorted(required_columns - set(df_dq.columns))
if missing_columns:
    raise RuntimeError(
        "DQDL row-level results are missing required source columns: "
        + ", ".join(missing_columns)
    )

if "DataQualityEvaluationResult" in df_dq.columns:
    is_invalid = F.col("DataQualityEvaluationResult") == "FAILED"
else:
    raise RuntimeError("DQDL row-level results are missing DataQualityEvaluationResult.")

for col in TABLE_CONFIG["data_quality"]["not_null"]:
    is_invalid = is_invalid | F.col(col).isNull()
for col in TABLE_CONFIG["data_quality"]["greater_than_zero"]:
    is_invalid = is_invalid | (F.col(col) <= 0)

allowed_products = TABLE_CONFIG["data_quality"].get("allowed_products", [])
if allowed_products:
    is_invalid = is_invalid | ~F.coalesce(
        F.col("product_name").isin(allowed_products), F.lit(False)
    )

allowed_branches = TABLE_CONFIG["data_quality"].get("allowed_branches", [])
if allowed_branches:
    allowed_branch_keys = [
        f"{branch['city']}|{branch['manager']}"
        for branch in allowed_branches
    ]
    branch_key = F.concat_ws(
        "|",
        F.coalesce(F.col("city"), F.lit("")),
        F.coalesce(F.col("manager"), F.lit("")),
    )
    is_invalid = is_invalid | ~F.coalesce(
        branch_key.isin(allowed_branch_keys), F.lit(False)
    )

dq_system_cols = [
    "DataQualityEvaluationResult",
    "DataQualityRulesPass",
    "DataQualityRulesFail",
    "DataQualityRulesSkip",
]
cleanup_dq_cols = [col for col in dq_system_cols if col in df_dq.columns]

df_quarantine = df_dq.filter(is_invalid).drop(*cleanup_dq_cols)
df_valid = df_dq.filter(~is_invalid).drop(*cols_to_drop).drop(*cleanup_dq_cols)

dedup_keys = TABLE_CONFIG["deduplication"]["primary_keys"]
order_by_col = TABLE_CONFIG["deduplication"]["order_by"]
dedup_tie_breaker = F.sha2(
    F.concat_ws(
        "||",
        *[F.coalesce(F.col(col).cast("string"), F.lit("")) for col in df_valid.columns]
    ),
    256,
)
logger.info(
    f"Deduplicating valid records on keys {dedup_keys} ordered by {order_by_col}..."
)

dedup_window = Window.partitionBy(*dedup_keys).orderBy(
    F.col(order_by_col).desc(),
    dedup_tie_breaker.desc(),
)
df_valid = (
    df_valid.withColumn("row_num", F.row_number().over(dedup_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

quarantine_count = df_quarantine.count()
valid_count = df_valid.count()

if quarantine_count > 0:
    logger.warning(f"Routing {quarantine_count} invalid records to quarantine: {QUARANTINE_PATH}")
    df_quarantine.write.mode("append").parquet(QUARANTINE_PATH)
else:
    logger.info("Zero invalid records detected.")

logger.info(f"Proceeding with {valid_count} valid records for domain modeling.")

current_ts = F.current_timestamp()
df_enriched = (
    df_valid
    .withColumn("total_amount", F.round(F.col("unit_price") * F.col("quantity"), 2))
    .withColumn("is_fractional_qty", (F.col("quantity") % 1) != 0)
    .withColumn("city", F.initcap(F.col("city")))
    .withColumn("month", F.month(F.col("order_date")))
    .withColumn("year", F.year(F.col("order_date")))
)

logger.info("Executing Iceberg MERGE INTO for Branch Dimension...")
dim_branch_df = create_dimension(
    df_enriched,
    TABLE_CONFIG["dimensions"]["branch"]["natural_keys"],
    "branch_id",
    current_ts,
)
dim_branch_df.createOrReplaceTempView("staged_dim_branch")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{TARGET_DATABASE}.dim_branch (
        branch_id STRING,
        city STRING,
        manager STRING,
        created_date TIMESTAMP,
        last_updated_date TIMESTAMP
    )
    USING iceberg
""")

spark.sql(f"""
    MERGE INTO glue_catalog.{TARGET_DATABASE}.dim_branch AS target
    USING staged_dim_branch AS source
    ON target.branch_id = source.branch_id
    WHEN MATCHED THEN
      UPDATE SET target.last_updated_date = source.last_updated_date
    WHEN NOT MATCHED THEN
      INSERT *
""")

logger.info("Executing Iceberg MERGE INTO for Product Dimension...")
dim_product_df = create_dimension(
    df_enriched,
    TABLE_CONFIG["dimensions"]["product"]["natural_keys"],
    "product_id",
    current_ts,
)
dim_product_df.createOrReplaceTempView("staged_dim_product")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{TARGET_DATABASE}.dim_product (
        product_id STRING,
        product_name STRING,
        created_date TIMESTAMP,
        last_updated_date TIMESTAMP
    )
    USING iceberg
""")

spark.sql(f"""
    MERGE INTO glue_catalog.{TARGET_DATABASE}.dim_product AS target
    USING staged_dim_product AS source
    ON target.product_id = source.product_id
    WHEN MATCHED THEN
      UPDATE SET target.last_updated_date = source.last_updated_date
    WHEN NOT MATCHED THEN
      INSERT *
""")

fact_cols = TABLE_CONFIG["fact_columns"]
fact_table_name = TABLE_CONFIG["fact_table_name"]
logger.info(f"Executing Iceberg MERGE INTO for Fact Table: {fact_table_name}...")
fact_df = (
    df_enriched
    .withColumn(
        "branch_id",
        generate_surrogate_key(TABLE_CONFIG["dimensions"]["branch"]["natural_keys"]),
    )
    .withColumn(
        "product_id",
        generate_surrogate_key(TABLE_CONFIG["dimensions"]["product"]["natural_keys"]),
    )
    .select("branch_id", "product_id", *fact_cols)
    .withColumn("created_date", current_ts)
    .withColumn("last_updated_date", current_ts)
)

fact_df.createOrReplaceTempView("staged_fact_sales")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{TARGET_DATABASE}.{fact_table_name} (
        branch_id STRING,
        product_id STRING,
        order_id STRING,
        order_date DATE,
        purchase_type STRING,
        payment_method STRING,
        unit_price DOUBLE,
        quantity DOUBLE,
        total_amount DOUBLE,
        is_fractional_qty BOOLEAN,
        month INT,
        year INT,
        created_date TIMESTAMP,
        last_updated_date TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (year, month)
""")

spark.sql(f"""
    MERGE INTO glue_catalog.{TARGET_DATABASE}.{fact_table_name} AS target
    USING staged_fact_sales AS source
    ON target.order_id = source.order_id
    WHEN MATCHED THEN
      UPDATE SET
        target.unit_price = source.unit_price,
        target.quantity = source.quantity,
        target.total_amount = source.total_amount,
        target.last_updated_date = source.last_updated_date
    WHEN NOT MATCHED THEN
      INSERT *
""")

logger.info("Successfully completed Iceberg upserts. Committing Glue Job state...")
job.commit()
logger.info("ETL Job Execution Finished Successfully.")
