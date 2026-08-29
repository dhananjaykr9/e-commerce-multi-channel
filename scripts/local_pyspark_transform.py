"""
Local PySpark ETL Transform Script
====================================
This is the local equivalent of: src/glue/ecommerce_glue_transform.py

The PySpark transformation logic is 100% identical to the Glue script.
The only difference is the I/O layer:
  - Glue reads/writes directly to s3:// (AWS handles credentials automatically)
  - This script uses boto3 to download raw files locally, runs Spark on local
    paths, then uploads curated Parquet back to S3 — avoiding all S3A/Hadoop
    Windows compatibility issues.

AWS Glue script (for cloud execution):  src/glue/ecommerce_glue_transform.py
This script     (for local execution):  scripts/local_pyspark_transform.py
"""

import os
import sys
import shutil
import argparse
import tempfile
from pathlib import Path
from dotenv import load_dotenv
import boto3

load_dotenv()

# ============================================================
# Windows: Set HADOOP_HOME before importing PySpark
# ============================================================
def setup_hadoop_home():
    if os.name != "nt":
        return
    hadoop_dir = Path(__file__).parent.parent / "hadoop"
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    winutils = bin_dir / "winutils.exe"
    hdll = bin_dir / "hadoop.dll"
    if not winutils.exists() or not hdll.exists():
        import requests
        base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
        for fname, dest in [("winutils.exe", winutils), ("hadoop.dll", hdll)]:
            if not dest.exists():
                print(f"Downloading {fname}...")
                r = requests.get(f"{base}/{fname}", timeout=30)
                r.raise_for_status()
                dest.write_bytes(r.content)
                print(f"  + {fname} downloaded")
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")
    print(f"HADOOP_HOME set to: {hadoop_dir}")

setup_hadoop_home()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, year, month, dayofmonth, current_timestamp, concat, abs, hash, split, coalesce

# ============================================================
# Config (mirrors Glue job parameters)
# ============================================================
S3_BUCKET         = os.getenv("S3_BUCKET", "e-commerce-multi-channel-dhananjay99")
RAW_PREFIX        = os.getenv("S3_RAW_PREFIX", "raw-landing").strip("/")
CURATED_PREFIX    = os.getenv("S3_CURATED_PREFIX", "curated").strip("/")
QUARANTINE_PREFIX = os.getenv("S3_QUARANTINE_PREFIX", "bad-records").strip("/")

# ============================================================
# boto3 helpers
# ============================================================
def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-south-1"),
    )

def download_prefix(s3, s3_prefix: str, local_dir: Path) -> int:
    """Download all S3 objects under s3_prefix into local_dir. Returns file count."""
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(s3_prefix):].lstrip("/")
            dest = local_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(S3_BUCKET, key, str(dest))
            count += 1
    return count

def upload_directory(s3, local_dir: Path, s3_prefix: str):
    """Upload all files under local_dir to S3 under s3_prefix."""
    for fpath in local_dir.rglob("*"):
        if fpath.is_file():
            rel = fpath.relative_to(local_dir)
            s3_key = f"{s3_prefix}/{rel}".replace("\\", "/")
            s3.upload_file(str(fpath), S3_BUCKET, s3_key)

# ============================================================
# Main ETL — logic identical to ecommerce_glue_transform.py
# ============================================================
def run_local_transform(target_date_str: str):
    from datetime import datetime
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    y = target_date.strftime("%Y")
    m = target_date.strftime("%m")
    d = target_date.strftime("%d")

    s3 = get_s3()

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp          = Path(tmp_str)
        raw_local    = tmp / "raw"
        curated_local = tmp / "curated"
        quarantine_local = tmp / "quarantine"

        # ── Download raw S3 files locally ───────────────────
        print("Downloading raw data from S3...")
        prefixes = {
            "mysql/orders":      f"{RAW_PREFIX}/mysql/orders/year={y}/month={m}/day={d}",
            "mysql/customers":   f"{RAW_PREFIX}/mysql/customers",
            "mysql/products":    f"{RAW_PREFIX}/mysql/products",
            "marketplace/sales": f"{RAW_PREFIX}/marketplace/sales/year={y}/month={m}/day={d}",
        }
        for folder, prefix in prefixes.items():
            local_sub = raw_local / folder
            local_sub.mkdir(parents=True, exist_ok=True)
            n = download_prefix(s3, prefix, local_sub)
            print(f"  {folder}: {n} file(s) downloaded")

        # ── Start local Spark (no S3A needed) ───────────────
        print("Starting Spark session...")
        spark = SparkSession.builder \
            .appName("LocalECommerceTransform") \
            .master("local[*]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        # Local paths (equivalent of Glue's s3:// paths)
        RAW_PATH       = str(raw_local) + "/"
        CURATED_PATH   = str(curated_local) + "/"
        QUARANTINE_PATH = str(quarantine_local) + "/"

        print("Local ETL Started")
        print(f"Raw Path:       {RAW_PATH}")
        print(f"Curated Path:   {CURATED_PATH}")
        print(f"Quarantine Path:{QUARANTINE_PATH}")

        try:
            # ============================================================
            # 1. PROCESS SALES DATA (FACT)
            #    — identical logic to ecommerce_glue_transform.py lines 38-126
            # ============================================================
            print("Processing Sales...")

            from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

            # Read Raw MySQL Orders (CSV)
            mysql_files = list((raw_local / "mysql/orders").glob("*.csv"))
            if mysql_files:
                mysql_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH + "mysql/orders/*.csv")
            else:
                schema_mysql = StructType([
                    StructField("order_id", IntegerType(), True),
                    StructField("customer_id", IntegerType(), True),
                    StructField("product_id", IntegerType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("total_amount", DoubleType(), True),
                    StructField("order_status", StringType(), True),
                    StructField("created_at", StringType(), True),
                ])
                mysql_raw = spark.createDataFrame([], schema_mysql)

            # Read Raw Marketplace Sales (JSON)
            mkt_files = list((raw_local / "marketplace/sales").rglob("*.json"))
            if mkt_files:
                mkt_raw = spark.read.option("recursiveFileLookup", "true").json(RAW_PATH + "marketplace/sales/")
            else:
                schema_mkt = StructType([
                    StructField("transaction_id", StringType(), True),
                    StructField("product_id", IntegerType(), True),
                    StructField("customer_email", StringType(), True),
                    StructField("marketplace_name", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("amount", DoubleType(), True),
                    StructField("transaction_date", StringType(), True),
                ])
                mkt_raw = spark.createDataFrame([], schema_mkt)

            # --- Validation and Cleaning ---
            # Valid MySQL: Non-null keys and positive quantities, not Cancelled
            valid_mysql = mysql_raw.filter(
                col("order_id").isNotNull() &
                col("customer_id").isNotNull() &
                col("product_id").isNotNull() &
                (col("quantity") > 0) &
                (col("order_status") != "Cancelled")
            )

            # Invalid MySQL to Quarantine (null keys, invalid quantities, or cancelled status)
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

            # Write Curated Sales (local Parquet)
            final_sales.write.mode("overwrite").partitionBy("year", "month", "day").parquet(CURATED_PATH + "sales/")
            print("Curated Sales written successfully.")

            # ============================================================
            # 2. PROCESS PRODUCTS DIMENSION
            #    — identical logic to ecommerce_glue_transform.py lines 131-143
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
            #    — identical logic to ecommerce_glue_transform.py lines 148-177
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

            # Write Curated Customers (local Parquet)
            deduplicated_customers.write.mode("overwrite").parquet(CURATED_PATH + "customers/")
            print("Curated Customers written successfully.")

        finally:
            spark.stop()
            print("Spark session stopped.")

        # ── Upload curated Parquet to S3 ────────────────────
        print("Uploading curated Parquet to S3...")
        for table in ["sales", "products", "customers"]:
            local_table = curated_local / table
            if local_table.exists():
                upload_directory(s3, local_table, f"{CURATED_PREFIX}/{table}")
                print(f"  {table} uploaded to s3://{S3_BUCKET}/{CURATED_PREFIX}/{table}/")

        # ── Upload quarantine if any ─────────────────────────
        if quarantine_local.exists():
            for qdir in quarantine_local.iterdir():
                if qdir.is_dir():
                    upload_directory(s3, qdir, f"{QUARANTINE_PREFIX}/{qdir.name}")
                    print(f"  Quarantine/{qdir.name} uploaded")

        print("Local ETL Completed Successfully")


# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local PySpark E-Commerce ETL (mirrors AWS Glue job)")
    parser.add_argument("--date", required=True, help="Target date to process (YYYY-MM-DD)")
    args = parser.parse_args()
    run_local_transform(args.date)
