import json

def lambda_handler(event, context):
    path = event.get('path', '')
    query_params = event.get('queryStringParameters') or {}
    date_str = query_params.get('date', '2026-08-19')
    
    print(f"Received request for path: {path} with date parameter: {date_str}")
    
    # Simple routing based on API Gateway path
    if path.endswith('/sales'):
        data = generate_sales(date_str)
        return build_response(200, data)
    elif path.endswith('/refunds'):
        data = generate_refunds(date_str)
        return build_response(200, data)
    else:
        return build_response(400, {
            "error": f"Invalid path: '{path}'. Use /sales or /refunds endpoint."
        })

def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }

def generate_sales(date_str):
    # Generates realistic mock marketplace sales records
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

def generate_refunds(date_str):
    # Generates realistic mock marketplace refund records
    return [
        {
            "refund_id": f"MKT-REF-{date_str}-201",
            "original_transaction_id": f"MKT-SALE-AMZ-{date_str}-103",
            "refund_amount": 150.00,
            "customer_email": "bob.johnson@email.com",
            "refund_date": f"{date_str} 19:30:00"
        }
    ]
