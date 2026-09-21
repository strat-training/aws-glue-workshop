import sys
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "S3_BUCKET", "DATABASE_NAME", "TARGET_DATABASE"]
)

BUCKET = args["S3_BUCKET"]
DATABASE_NAME = args["DATABASE_NAME"]
TARGET_DATABASE = args["TARGET_DATABASE"]

GOLD_WAREHOUSE_PATH = f"{BUCKET}/03_gold/"
SILVER_WAREHOUSE_PATH = f"{BUCKET}/02_silver/"

sc = SparkContext()
glueContext = GlueContext(sc)

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", GOLD_WAREHOUSE_PATH)
    .getOrCreate()
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f"Reading Silver Iceberg Table: glue_catalog.{DATABASE_NAME}.fact_sales")
fact_sales = spark.read.table(f"glue_catalog.{DATABASE_NAME}.fact_sales")

logger.info("Processing KPI 1/5: Branch Performance (Price vs Volume)")
branch_window = Window.partitionBy("branch_id").orderBy("month")

branch_monthly = (
    fact_sales
    .withColumn("month", f.date_trunc("month", "order_date"))
    .groupBy("branch_id", "month")
    .agg(
        f.sum("total_amount").alias("revenue"),
        f.sum("quantity").alias("total_qty")
    )
    .withColumn("avg_price", f.col("revenue") / f.col("total_qty"))
    .select("branch_id", "month", "revenue", "total_qty", "avg_price")
)

price_volume_decomp = (
    branch_monthly
    .withColumn("prior_revenue", f.coalesce(f.lag("revenue").over(branch_window), f.lit(0)))
    .withColumn("prior_qty", f.coalesce(f.lag("total_qty").over(branch_window), f.lit(0)))
    .withColumn("prior_price", f.coalesce(f.lag("avg_price").over(branch_window), f.lit(0)))
    .withColumn("revenue_change", f.col("revenue") - f.col("prior_revenue"))
    .withColumn("qty_change", f.col("total_qty") - f.col("prior_qty"))
    .withColumn("price_change", f.col("avg_price") - f.col("prior_price"))
    .withColumn("volume_effect", f.col("qty_change") * f.col("prior_price"))
    .withColumn("price_effect", f.col("price_change") * f.col("total_qty"))
    .withColumn("as_of_date", f.current_timestamp())
    .select("branch_id", "month", "revenue_change", "volume_effect", "price_effect", "as_of_date")
)

price_volume_decomp.writeTo(f"glue_catalog.{TARGET_DATABASE}.branch_performance") \
    .tableProperty("format-version", "2") \
    .options(path=f"{GOLD_WAREHOUSE_PATH}branch_performance") \
    .createOrReplace()

logger.info(f"Created Gold Iceberg table: glue_catalog.{TARGET_DATABASE}.branch_performance")

logger.info("Processing KPI 2/5: Menu Quadrants")
w_rev = Window.partitionBy("branch_id").orderBy(f.col("revenue"))
w_vol = Window.partitionBy("branch_id").orderBy(f.col("volume"))

product_branch = (
    fact_sales
    .groupBy("branch_id", "product_id")
    .agg(
        f.sum("total_amount").alias("revenue"),
        f.sum("quantity").alias("volume")
    )
    .withColumn("revenue_tier", f.ntile(2).over(w_rev))
    .withColumn("volume_tier", f.ntile(2).over(w_vol))
    .select("branch_id", "product_id", "revenue", "volume", "revenue_tier", "volume_tier")
)

menu_quadrant = (
    product_branch
    .withColumn(
        "quadrant",
        f.when((f.col("revenue_tier") == 2) & (f.col("volume_tier") == 2), "core")
        .when((f.col("revenue_tier") == 2) & (f.col("volume_tier") == 1), "premium/niche")
        .when((f.col("revenue_tier") == 1) & (f.col("volume_tier") == 2), "value driver")
        .otherwise("underperformer")
    )
    .withColumn("as_of_date", f.current_timestamp())
    .select("branch_id", "product_id", "revenue", "volume", "revenue_tier", "volume_tier", "quadrant", "as_of_date")
)

menu_quadrant.writeTo(f"glue_catalog.{TARGET_DATABASE}.menu_quadrant") \
    .tableProperty("format-version", "2") \
    .options(path=f"{GOLD_WAREHOUSE_PATH}menu_quadrant") \
    .createOrReplace()

logger.info(f"Created Gold Iceberg table: glue_catalog.{TARGET_DATABASE}.menu_quadrant")

logger.info("Processing KPI 3/5: Menu Rank Volatility")
w_rank = Window.partitionBy("branch_id").orderBy(f.col("revenue").desc())

ranked_products = (
    fact_sales
    .groupBy("branch_id", "product_id")
    .agg(f.sum("total_amount").alias("revenue"))
    .withColumn("revenue_rank", f.rank().over(w_rank))
    .select("branch_id", "product_id", "revenue_rank")
)

menu_rank_volatility = (
    ranked_products
    .groupBy("product_id")
    .agg(f.stddev("revenue_rank").alias("rank_volatility"))
    .orderBy(f.col("rank_volatility").desc())
    .withColumn("as_of_date", f.current_timestamp())
    .select("product_id", "rank_volatility", "as_of_date")
)

menu_rank_volatility.writeTo(f"glue_catalog.{TARGET_DATABASE}.menu_rank_volatility") \
    .tableProperty("format-version", "2") \
    .options(path=f"{GOLD_WAREHOUSE_PATH}menu_rank_volatility") \
    .createOrReplace()

logger.info(f"Created Gold Iceberg table: glue_catalog.{TARGET_DATABASE}.menu_rank_volatility")

logger.info("Processing KPI 4/5: Channel Mix")
w_branch = Window.partitionBy("branch_id")

channel_mix = (
    fact_sales
    .groupBy("branch_id", "purchase_type")
    .agg(
        f.count("*").alias("orders"),
        f.sum("total_amount").alias("revenue")
    )
    .withColumn("branch_total_revenue", f.sum("revenue").over(w_branch))
    .withColumn("pct_of_branch_revenue", f.col("revenue") / f.col("branch_total_revenue"))
    .withColumn("as_of_date", f.current_timestamp())
    .select("branch_id", "purchase_type", "orders", "revenue", "pct_of_branch_revenue", "as_of_date")
)

channel_mix.writeTo(f"glue_catalog.{TARGET_DATABASE}.channel_mix") \
    .tableProperty("format-version", "2") \
    .options(path=f"{GOLD_WAREHOUSE_PATH}channel_mix") \
    .createOrReplace()

logger.info(f"Created Gold Iceberg table: glue_catalog.{TARGET_DATABASE}.channel_mix")

logger.info("Processing KPI 5/5: Channel Growth Correlation")
branch_online_share = (
    fact_sales
    .groupBy("branch_id")
    .agg(
        (
            f.sum(f.when(f.col("purchase_type") == "Online", f.col("total_amount")).otherwise(0))
            / f.sum("total_amount")
        ).alias("pct_online_revenue")
    )
)

branch_growth_rate = (
    branch_monthly
    .withColumn("prior_revenue", f.coalesce(f.lag("revenue").over(branch_window), f.lit(0)))
    .withColumn("mom_growth_rate", (f.col("revenue") - f.col("prior_revenue")) / f.col("prior_revenue"))
    .groupBy("branch_id")
    .agg(f.avg("mom_growth_rate").alias("revenue_growth_rate"))
)

channel_growth_correlation = (
    branch_online_share
    .join(branch_growth_rate, on="branch_id", how="inner")
    .select(f.corr("pct_online_revenue", "revenue_growth_rate").alias("channel_growth_correlation"))
    .withColumn("as_of_date", f.current_timestamp())
)

channel_growth_correlation.writeTo(f"glue_catalog.{TARGET_DATABASE}.channel_growth_correlation") \
    .tableProperty("format-version", "2") \
    .options(path=f"{GOLD_WAREHOUSE_PATH}channel_growth_correlation") \
    .createOrReplace()

logger.info(f"Created Gold Iceberg table: glue_catalog.{TARGET_DATABASE}.channel_growth_correlation")

job.commit()
logger.info("Gold Layer KPI generation completed successfully.")
