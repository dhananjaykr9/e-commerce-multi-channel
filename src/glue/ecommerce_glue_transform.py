import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, to_timestamp, year, month, dayofmonth, current_timestamp, concat, abs, hash, split, coalesce

# ============================================================
# Initialize Glue Job
# ============================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "RAW_PREFIX", "CURATED_PREFIX", "QUARANTINE_PREFIX"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

s3_bucket = args["S3_BUCKET"]
raw_prefix = args["RAW_PREFIX"].strip("/")
curated_prefix = args["CURATED_PREFIX"].strip("/")
quarantine_prefix = args["QUARANTINE_PREFIX"].strip("/")

RAW_PATH = f"s3://{s3_bucket}/{raw_prefix}/"
CURATED_PATH = f"s3://{s3_bucket}/{curated_prefix}/"
QUARANTINE_PATH = f"s3://{s3_bucket}/{quarantine_prefix}/"

print("Glue Job Started")
print(f"Bucket: {s3_bucket}")
print(f"Raw Path: {RAW_PATH}")
print(f"Curated Path: {CURATED_PATH}")
print(f"Quarantine Path: {QUARANTINE_PATH}")

# ============================================================
# 1. PROCESS SALES DATA (FACT)
# ============================================================
print("Processing Sales...")

# Read Raw MySQL Orders (CSV)
mysql_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH + "mysql/orders/*/*/*/*.csv")

# Read Raw Marketplace Sales (JSON)
mkt_raw = spark.read.option("recursiveFileLookup", "true").json(RAW_PATH + "marketplace/sales/")

# --- Validation and Cleaning ---
# Valid MySQL: Non-null keys and positive quantities, not Cancelled
valid_mysql = mysql_raw.filter(
    col("order_id").isNotNull() & 
    col("customer_id").isNotNull() & 
    col("product_id").isNotNull() & 
    (col("quantity") > 0) & 
    (col("order_status") != "Cancelled")
)

# Invalid MySQL to Quarantine
invalid_mysql = mysql_raw.filter(
    col("order_id").isNull() | 
    col("customer_id").isNull() | 
    col("product_id").isNull() | 
    (col("quantity") <= 0) |
    (col("order_status") == "Cancelled")
)
if invalid_mysql.count() > 0:
    invalid_mysql.write.mode("append").json(QUARANTINE_PATH + "mysql_orders/")

# Standardize MySQL fields
mysql_sales = valid_mysql.select(
    concat(lit("MYSQL-ORDER-"), col("order_id")).alias("sale_id"),
    col("product_id").cast("int").alias("product_id"),
    col("customer_id").cast("int").alias("customer_id"),
    lit(None).cast("string").alias("customer_email"),
    lit("Website").alias("channel_name"),
    col("quantity").cast("int").alias("quantity_sold"),
    col("total_amount").cast("double").alias("sales_amount"),
    coalesce(
        to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"), 
        current_timestamp()
    ).alias("transaction_date")
)

# Valid Marketplace: Non-null keys and positive quantities
valid_mkt = mkt_raw.filter(
    col("transaction_id").isNotNull() & 
    col("product_id").isNotNull() & 
    col("customer_email").isNotNull() & 
    (col("quantity") > 0)
)

# Invalid Marketplace to Quarantine
invalid_mkt = mkt_raw.filter(
    col("transaction_id").isNull() | 
    col("product_id").isNull() | 
    col("customer_email").isNull() | 
    (col("quantity") <= 0)
)
if invalid_mkt.count() > 0:
    invalid_mkt.write.mode("append").json(QUARANTINE_PATH + "marketplace_sales/")

# Standardize Marketplace fields
mkt_sales = valid_mkt.select(
    col("transaction_id").alias("sale_id"),
    col("product_id").cast("int").alias("product_id"),
    abs(hash(col("customer_email"))).cast("int").alias("customer_id"),
    col("customer_email"),
    col("marketplace_name").alias("channel_name"),
    col("quantity").cast("int").alias("quantity_sold"),
    col("amount").cast("double").alias("sales_amount"),
    to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss").alias("transaction_date")
)

# --- Union and Deduplicate ---
unified_sales = mysql_sales.unionByName(mkt_sales, allowMissingColumns=True)
deduplicated_sales = unified_sales.dropDuplicates(["sale_id"])

# Add Partition Columns
final_sales = deduplicated_sales.withColumn(
    "year", year(col("transaction_date"))
).withColumn(
    "month", month(col("transaction_date"))
).withColumn(
    "day", dayofmonth(col("transaction_date"))
)

# Write Curated Sales to S3
final_sales.write.mode("overwrite").partitionBy("year", "month", "day").parquet(CURATED_PATH + "sales/")
print("Curated Sales written successfully.")

# ============================================================
# 2. PROCESS PRODUCTS DIMENSION
# ============================================================
print("Processing Products...")
products_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH + "mysql/products/*.csv")

curated_products = products_raw.select(
    col("product_id").cast("int").alias("product_id"),
    col("product_name"),
    col("category"),
    col("price").cast("double").alias("price"),
    lit("MySQL").alias("source_system")
)

curated_products.write.mode("overwrite").parquet(CURATED_PATH + "products/")
print("Curated Products written successfully.")

# ============================================================
# 3. PROCESS CUSTOMERS DIMENSION
# ============================================================
print("Processing Customers...")

# MySQL Customers
mysql_cust = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH + "mysql/customers/*.csv")
mysql_customers = mysql_cust.select(
    col("customer_id").cast("int").alias("customer_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("phone"),
    lit("MySQL").alias("source_system")
)

# Marketplace Customers (Extracted from sales)
mkt_customers = valid_mkt.select(
    abs(hash(col("customer_email"))).cast("int").alias("customer_id"),
    split(col("customer_name"), " ")[0].alias("first_name"),
    coalesce(split(col("customer_name"), " ")[1], lit("")).alias("last_name"),
    col("customer_email").alias("email"),
    lit(None).cast("string").alias("phone"),
    col("marketplace_name").alias("source_system")
).dropDuplicates(["email"])

# Union Customers
unified_customers = mysql_customers.unionByName(mkt_customers, allowMissingColumns=True)
deduplicated_customers = unified_customers.dropDuplicates(["customer_id"])

# Write Curated Customers to S3
deduplicated_customers.write.mode("overwrite").parquet(CURATED_PATH + "customers/")
print("Curated Customers written successfully.")

# ============================================================
# Commit Job
# ============================================================
job.commit()
print("Glue Job Completed Successfully")
