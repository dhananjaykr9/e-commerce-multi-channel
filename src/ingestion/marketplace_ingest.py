import os
import json
import argparse
from datetime import datetime
import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def fetch_marketplace_data(endpoint_url, params=None):
    try:
        response = requests.get(endpoint_url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Warning: API request failed with status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Warning: Connection to API failed: {e}")
        return None

def generate_local_mock_sales(date_str):
    return [
        {
            "transaction_id": f"MKT-SALE-AMZ-{date_str}-101",
            "marketplace_name": "Amazon",
            "product_id": 1,
            "quantity": 1,
            "amount": 1200.00,
            "customer_name": "John Doe",
            "customer_email": "john.doe@email.com",
            "transaction_date": f"{date_str} 10:30:00"
        },
        {
            "transaction_id": f"MKT-SALE-FLP-{date_str}-102",
            "marketplace_name": "Flipkart",
            "product_id": 3,
            "quantity": 2,
            "amount": 500.00,
            "customer_name": "Jane Smith",
            "customer_email": "jane.smith@email.com",
            "transaction_date": f"{date_str} 14:15:00"
        },
        {
            "transaction_id": f"MKT-SALE-AMZ-{date_str}-103",
            "marketplace_name": "Amazon",
            "product_id": 4,
            "quantity": 1,
            "amount": 150.00,
            "customer_name": "Bob Johnson",
            "customer_email": "bob.johnson@email.com",
            "transaction_date": f"{date_str} 18:45:00"
        }
    ]

def generate_local_mock_refunds(date_str):
    return [
        {
            "refund_id": f"MKT-REF-{date_str}-201",
            "original_transaction_id": f"MKT-SALE-AMZ-{date_str}-103",
            "refund_amount": 150.00,
            "customer_email": "bob.johnson@email.com",
            "refund_date": f"{date_str} 19:30:00"
        }
    ]

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

def save_json_file(data, file_path):
    """Save data as newline-delimited JSON (NDJSON) — one object per line.
    This is the format required by Spark's spark.read.json().
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    records = data if isinstance(data, list) else [data]
    with open(file_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    print(f"Saved {len(records)} record(s) to {file_path}")

def main():
    parser = argparse.ArgumentParser(description="Ingest marketplace transactions from Lambda API to S3 Raw Landing")
    parser.add_argument("--date", type=str, help="Target date for ingestion (YYYY-MM-DD)", default=None)
    args = parser.parse_args()
    
    target_date_str = args.date
    if not target_date_str:
        target_date_str = datetime.today().strftime("%Y-%m-%d")
        
    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{target_date_str}'. Use YYYY-MM-DD.")
        return
        
    print(f"Starting ingestion process for marketplace date: {target_date_str}")
    
    # S3 and API configurations
    s3_bucket = os.getenv("S3_BUCKET", "e-commerce-multi-channel-dhananjay99")
    raw_prefix = os.getenv("S3_RAW_PREFIX", "raw-landing").strip("/")
    api_base_url = os.getenv("MARKETPLACE_API_URL", "")
    
    local_base_dir = os.path.join("data_staging", "marketplace")
    
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    
    # --- A. Ingest Marketplace Sales ---
    sales_data = None
    if api_base_url and "placeholder" not in api_base_url:
        sales_url = f"{api_base_url.rstrip('/')}/sales"
        print(f"Fetching sales data from: {sales_url}")
        sales_data = fetch_marketplace_data(sales_url, params={"date": target_date_str})
        
    if not sales_data:
        print("[API Pending/Offline] Generating local mock sales data...")
        sales_data = generate_local_mock_sales(target_date_str)
        
    sales_file = os.path.join(local_base_dir, f"sales_{target_date_str}.json")
    save_json_file(sales_data, sales_file)
    
    sales_s3_key = f"{raw_prefix}/marketplace/sales/year={year}/month={month}/day={day}/sales_{target_date_str}.json"
    upload_to_s3(sales_file, s3_bucket, sales_s3_key)
    os.remove(sales_file)
    
    # --- B. Ingest Marketplace Refunds ---
    refunds_data = None
    if api_base_url and "placeholder" not in api_base_url:
        refunds_url = f"{api_base_url.rstrip('/')}/refunds"
        print(f"Fetching refunds data from: {refunds_url}")
        refunds_data = fetch_marketplace_data(refunds_url, params={"date": target_date_str})
        
    if not refunds_data:
        print("[API Pending/Offline] Generating local mock refunds data...")
        refunds_data = generate_local_mock_refunds(target_date_str)
        
    refunds_file = os.path.join(local_base_dir, f"refunds_{target_date_str}.json")
    save_json_file(refunds_data, refunds_file)
    
    refunds_s3_key = f"{raw_prefix}/marketplace/refunds/year={year}/month={month}/day={day}/refunds_{target_date_str}.json"
    upload_to_s3(refunds_file, s3_bucket, refunds_s3_key)
    os.remove(refunds_file)
    
    # Cleanup staging dir
    try:
        os.rmdir(local_base_dir)
        os.rmdir(os.path.dirname(local_base_dir))
    except OSError:
        pass

if __name__ == "__main__":
    main()
