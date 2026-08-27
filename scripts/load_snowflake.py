import os
import argparse
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def run_load_pipeline(target_date_str):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    # Load Snowflake credentials
    sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
    sf_user = os.getenv("SNOWFLAKE_USER")
    sf_password = os.getenv("SNOWFLAKE_PASSWORD")
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH")
    sf_database = os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW")
    sf_schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")

    # Load AWS S3 credentials and configuration
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_bucket = os.getenv("S3_BUCKET", "e-commerce-multi-channel-dhananjay99")
    curated_prefix = os.getenv("S3_CURATED_PREFIX", "curated").strip("/")

    # Guard checks
    if not sf_account or "placeholder" in sf_account:
        raise ValueError("Error: SNOWFLAKE_ACCOUNT is not configured in .env")
    if not aws_access_key or "placeholder" in aws_access_key:
        raise ValueError("Error: AWS_ACCESS_KEY_ID is not configured in .env")

    print(f"Connecting to Snowflake database {sf_database}...")
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )
    cursor = conn.cursor()
    print("Connected successfully!")

    try:
        stage_name = "curated_s3_stage"
        
        # Setup S3 stage pointing to the curated prefix folder in the single bucket
        s3_url = f"s3://{s3_bucket}/{curated_prefix}"
        print(f"Setting up external stage pointing to {s3_url}...")
        cursor.execute(f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{s3_url}'
            CREDENTIALS = (AWS_KEY_ID = '{aws_access_key}' AWS_SECRET_KEY = '{aws_secret_key}');
        """)

        stage_prefix = f"@{stage_name}"

        # ==========================================
        # 1. LOAD PRODUCTS DIMENSION
        # ==========================================
        print("Loading products dimension...")
        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE temp_products_staging (
                product_id INT,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                source_system VARCHAR(50)
            );
        """)
        
        products_stage_path = f"{stage_prefix}/products/"
        print(f"Copying products from {products_stage_path}...")
        cursor.execute(f"""
            COPY INTO temp_products_staging
            FROM (
                SELECT $1:product_id::int, $1:product_name::string, $1:category::string, $1:price::decimal(10,2), $1:source_system::string
                FROM {products_stage_path}
            )
            FILE_FORMAT = (TYPE = parquet);
        """)
        
        # Merge products into dim_products
        cursor.execute("""
            MERGE INTO dim_products t
            USING temp_products_staging s
            ON t.product_id = s.product_id AND t.source_system = s.source_system
            WHEN MATCHED THEN
                UPDATE SET t.product_name = s.product_name, t.category = s.category, t.price = s.price
            WHEN NOT MATCHED THEN
                INSERT (product_id, product_name, category, price, source_system)
                VALUES (s.product_id, s.product_name, s.category, s.price, s.source_system);
        """)
        print("Products dimension load and merge completed.")

        # ==========================================
        # 2. LOAD CUSTOMERS DIMENSION
        # ==========================================
        print("Loading customers dimension...")
        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE temp_customers_staging (
                customer_id INT,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                phone VARCHAR(20),
                source_system VARCHAR(50)
            );
        """)
        
        customers_stage_path = f"{stage_prefix}/customers/"
        print(f"Copying customers from {customers_stage_path}...")
        cursor.execute(f"""
            COPY INTO temp_customers_staging
            FROM (
                SELECT $1:customer_id::int, $1:first_name::string, $1:last_name::string, $1:email::string, $1:phone::string, $1:source_system::string
                FROM {customers_stage_path}
            )
            FILE_FORMAT = (TYPE = parquet);
        """)
        
        # Merge customers into dim_customers
        cursor.execute("""
            MERGE INTO dim_customers t
            USING temp_customers_staging s
            ON t.customer_id = s.customer_id AND t.source_system = s.source_system
            WHEN MATCHED THEN
                UPDATE SET t.first_name = s.first_name, t.last_name = s.last_name, t.email = s.email, t.phone = s.phone
            WHEN NOT MATCHED THEN
                INSERT (customer_id, first_name, last_name, email, phone, source_system)
                VALUES (s.customer_id, s.first_name, s.last_name, s.email, s.phone, s.source_system);
        """)
        print("Customers dimension load and merge completed.")

        # ==========================================
        # 3. LOAD SALES FACT TABLE
        # ==========================================
        print("Loading sales fact table...")
        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE temp_sales_staging (
                sale_id VARCHAR(100),
                product_id INT,
                customer_id INT,
                customer_email VARCHAR(100),
                channel_name VARCHAR(50),
                quantity_sold INT,
                sales_amount DECIMAL(10,2),
                refund_amount DECIMAL(10,2),
                transaction_date TIMESTAMP
            );
        """)
        
        sales_stage_path = f"{stage_prefix}/sales/year={year}/month={month}/day={day}/"
        print(f"Copying sales from {sales_stage_path}...")
        cursor.execute(f"""
            COPY INTO temp_sales_staging
            FROM (
                SELECT 
                    $1:sale_id::string, 
                    $1:product_id::int, 
                    $1:customer_id::int, 
                    $1:customer_email::string, 
                    $1:channel_name::string, 
                    $1:quantity_sold::int, 
                    $1:sales_amount::decimal(10,2), 
                    $1:refund_amount::decimal(10,2), 
                    $1:transaction_date::timestamp
                FROM {sales_stage_path}
            )
            FILE_FORMAT = (TYPE = parquet);
        """)

        # Clean existing fact records for the target date to ensure idempotency
        print(f"Cleaning existing fact_sales records for date {target_date_str}...")
        cursor.execute(f"""
            DELETE FROM fact_sales 
            WHERE DATE(transaction_date) = '{target_date_str}';
        """)

        # Insert new fact records mapping keys from staging
        print("Inserting records into fact_sales table mapping dimensions keys...")
        cursor.execute(f"""
            INSERT INTO fact_sales (
                sale_id, product_key, customer_key, channel_key, 
                quantity_sold, sales_amount, refund_amount, transaction_date
            )
            SELECT 
                s.sale_id,
                p.product_key,
                c.customer_key,
                ch.channel_key,
                s.quantity_sold,
                s.sales_amount,
                s.refund_amount,
                s.transaction_date
            FROM temp_sales_staging s
            -- Map dim_products key
            LEFT JOIN dim_products p 
                ON s.product_id = p.product_id 
                AND p.source_system = CASE WHEN s.channel_name = 'Website' THEN 'MySQL' ELSE 'Marketplace API' END
            -- Map dim_customers key
            LEFT JOIN dim_customers c 
                ON s.customer_id = c.customer_id 
                AND c.source_system = CASE WHEN s.channel_name = 'Website' THEN 'MySQL' ELSE s.channel_name END
            -- Map dim_channels key
            LEFT JOIN dim_channels ch 
                ON s.channel_name = ch.channel_name;
        """)
        print("Sales fact table load completed successfully.")

    finally:
        cursor.close()
        conn.close()
        print("Snowflake connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Curated S3 Data into Snowflake Star Schema")
    parser.add_argument("--date", type=str, required=True, help="Target date to load (YYYY-MM-DD)")
    args = parser.parse_args()
    
    run_load_pipeline(args.date)
