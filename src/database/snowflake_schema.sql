-- Snowflake Star Schema Schema Definition

-- 1. Create Warehouse (if not exists)
CREATE WAREHOUSE IF NOT EXISTS ECOMMERCE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- 2. Create Database and Schema
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW;
USE DATABASE ECOMMERCE_DW;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- 3. Create Dimension: dim_channels
CREATE TABLE IF NOT EXISTS dim_channels (
    channel_key INT IDENTITY(1,1) PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL, -- 'Website', 'Amazon', 'Flipkart', etc.
    channel_type VARCHAR(20) NOT NULL  -- 'Internal', 'Marketplace'
);

-- 4. Create Dimension: dim_products
CREATE TABLE IF NOT EXISTS dim_products (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,           -- Natural key from source systems
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    source_system VARCHAR(50) NOT NULL  -- 'MySQL', 'Marketplace API'
);

-- 5. Create Dimension: dim_customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,          -- Natural key from source systems (or marketplace user id)
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    source_system VARCHAR(50) NOT NULL  -- 'MySQL', 'Marketplace API'
);

-- 6. Create Fact Table: fact_sales
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id VARCHAR(100) PRIMARY KEY,  -- Source order_id / transaction_id
    product_key INT REFERENCES dim_products(product_key),
    customer_key INT REFERENCES dim_customers(customer_key),
    channel_key INT REFERENCES dim_channels(channel_key),
    quantity_sold INT NOT NULL,
    sales_amount DECIMAL(10, 2) NOT NULL,
    refund_amount DECIMAL(10, 2) DEFAULT 0.00,
    transaction_date TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Seed dim_channels (since channels are static and known)
TRUNCATE TABLE dim_channels;
INSERT INTO dim_channels (channel_name, channel_type) VALUES
('Website', 'Internal'),
('Amazon', 'Marketplace'),
('Flipkart', 'Marketplace');
