import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_bucket():
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
    
    if not aws_access_key or "placeholder" in aws_access_key:
        print("Error: AWS_ACCESS_KEY_ID is not configured in .env")
        return
    if not aws_secret_key or "placeholder" in aws_secret_key:
        print("Error: AWS_SECRET_ACCESS_KEY is not configured in .env")
        return

    bucket = os.getenv("S3_BUCKET", "e-commerce-multi-channel-dhananjay99")
    
    print(f"Connecting to AWS in region {region}...")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )
    
    print(f"Creating S3 bucket: {bucket}...")
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket)
        else:
            s3_client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"S3 bucket '{bucket}' created successfully.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "BucketAlreadyOwnedByYou":
            print(f"S3 bucket '{bucket}' already exists and is owned by you.")
        elif error_code == "BucketAlreadyExists":
            print(f"Error: Bucket name '{bucket}' is already taken globally. Please choose a different name.")
        else:
            print(f"Failed to create S3 bucket '{bucket}': {e}")

if __name__ == "__main__":
    create_bucket()
