import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def apply_schema():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    
    if not account or "placeholder" in account:
        print("Error: SNOWFLAKE_ACCOUNT is not configured in .env")
        return
    if not user or "placeholder" in user:
        print("Error: SNOWFLAKE_USER is not configured in .env")
        return
    if not password or "placeholder" in password:
        print("Error: SNOWFLAKE_PASSWORD is not configured in .env")
        return

    sql_file_path = os.path.join("src", "database", "snowflake_schema.sql")
    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file not found at {sql_file_path}")
        return

    print("Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
        )
        cursor = conn.cursor()
        print("Connected successfully!")
        
        print(f"Reading DDL from {sql_file_path}...")
        with open(sql_file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Split statements by semicolon, filtering out empty ones
        # Simple parser that handles multi-line statements
        statements = []
        current_statement = []
        for line in sql_content.splitlines():
            # Strip comments
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith("--"):
                continue
            current_statement.append(line)
            if stripped_line.endswith(";"):
                statements.append("\n".join(current_statement))
                current_statement = []

        print(f"Found {len(statements)} SQL statements to execute.")
        
        for i, stmt in enumerate(statements, 1):
            stmt = stmt.strip()
            if not stmt:
                continue
            # Print a snippet of the statement for logging
            first_line = stmt.splitlines()[0][:60]
            print(f"[{i}/{len(statements)}] Executing: {first_line}...")
            cursor.execute(stmt)
            
        print("Snowflake schema DDL applied successfully!")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error executing SQL statements: {e}")

if __name__ == "__main__":
    apply_schema()
