import os
import csv
import argparse
from datetime import datetime
import pymysql
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_mysql_connection():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3307)),
        user=os.getenv("MYSQL_USER", "ecommerce_user"),
        password=os.getenv("MYSQL_PASSWORD", "ecommerce_password"),
        database=os.getenv("MYSQL_DATABASE", "ecommerce_db"),
        cursorclass=pymysql.cursors.DictCursor
    )

def extract_table_to_csv(connection, query, file_path, params=None):
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
    if not rows:
        print(f"No records found for query: {query[:50]}...")
        return False
        
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Write to CSV
    keys = rows[0].keys()
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        # Convert datetime objects to string for serialization
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.strftime("%Y-%m-%d %H:%M:%S")
            dict_writer.writerow(row)
            
    print(f"Extracted {len(rows)} records to {file_path}")
    return True

def upload_to_s3(local_file, bucket, s3_key):
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not aws_access_key or "placeholder" in aws_access_key:
        print(f"[AWS Config Pending] Skipped uploading {local_file} to s3://{bucket}/{s3_key}")
        return False
        
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        s3_client.upload_file(local_file, bucket, s3_key)
        print(f"Successfully uploaded {local_file} to s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        print(f"Failed to upload to S3: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Extract data from local MySQL to S3 Raw Landing")
    parser.add_argument("--date", type=str, help="Target date for extraction (YYYY-MM-DD)", default=None)
    args = parser.parse_args()
    
    target_date_str = args.date
    if not target_date_str:
        target_date_str = datetime.today().strftime("%Y-%m-%d")
        
    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{target_date_str}'. Use YYYY-MM-DD.")
        return
        
    print(f"Starting extraction process for date: {target_date_str}")
    
    # Define local staging directory paths (used for temp storage before upload)
    local_base_dir = os.path.join("data_staging", "mysql")
    
    try:
        conn = get_mysql_connection()
        print("Connected to MySQL operational database successfully.")
    except Exception as e:
        print(f"Error connecting to MySQL database: {e}")
        return
        
    # S3 configurations (Single Bucket with Prefix)
    s3_bucket = os.getenv("S3_BUCKET", "e-commerce-multi-channel-dhananjay99")
    raw_prefix = os.getenv("S3_RAW_PREFIX", "raw-landing").strip("/")
    
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    
    try:
        # --- A. Extract Customers (Full Extract) ---
        customers_file = os.path.join(local_base_dir, "customers.csv")
        customers_query = "SELECT customer_id, first_name, last_name, email, phone, created_at FROM customers"
        if extract_table_to_csv(conn, customers_query, customers_file):
            s3_key = f"{raw_prefix}/mysql/customers/customers.csv"
            upload_to_s3(customers_file, s3_bucket, s3_key)
            os.remove(customers_file)
            
        # --- B. Extract Products (Full Extract) ---
        products_file = os.path.join(local_base_dir, "products.csv")
        products_query = "SELECT product_id, product_name, category, price, created_at FROM products"
        if extract_table_to_csv(conn, products_query, products_file):
            s3_key = f"{raw_prefix}/mysql/products/products.csv"
            upload_to_s3(products_file, s3_bucket, s3_key)
            os.remove(products_file)
            
        # --- C. Extract Orders (Incremental Extract by Date) ---
        orders_file = os.path.join(local_base_dir, f"orders_{target_date_str}.csv")
        orders_query = """
            SELECT order_id, customer_id, product_id, quantity, total_amount, order_status, created_at 
            FROM orders 
            WHERE DATE(created_at) = %s
        """
        if extract_table_to_csv(conn, orders_query, orders_file, params=(target_date_str,)):
            s3_key = f"{raw_prefix}/mysql/orders/year={year}/month={month}/day={day}/orders_{target_date_str}.csv"
            upload_to_s3(orders_file, s3_bucket, s3_key)
            os.remove(orders_file)
            
    finally:
        conn.close()
        print("MySQL connection closed.")
        # Cleanup staging dir if empty
        try:
            os.rmdir(local_base_dir)
            os.rmdir(os.path.dirname(local_base_dir))
        except OSError:
            pass

if __name__ == "__main__":
    main()
