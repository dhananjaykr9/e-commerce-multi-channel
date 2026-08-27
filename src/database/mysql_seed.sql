-- Seed E-Commerce Operational Database (MySQL) with Mock Data

USE ecommerce_db;

-- Clear existing data (to allow re-seeding)
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE orders;
TRUNCATE TABLE customers;
TRUNCATE TABLE products;
SET FOREIGN_KEY_CHECKS = 1;

-- 1. Seed Products
INSERT INTO products (product_id, product_name, category, price) VALUES
(1, 'UltraBook Pro 15', 'Electronics', 1200.00),
(2, 'SmartPhone X10', 'Electronics', 800.00),
(3, 'Wireless Noise-Cancelling Headphones', 'Audio', 250.00),
(4, 'FitLife Smartwatch', 'Wearables', 150.00),
(5, 'TabLite 10-inch', 'Electronics', 350.00);

-- 2. Seed Customers
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at) VALUES
(1, 'Aarav', 'Sharma', 'aarav.sharma@email.com', '9876543210', '2026-08-01 10:00:00'),
(2, 'Diya', 'Patel', 'diya.patel@email.com', '9876543211', '2026-08-02 11:30:00'),
(3, 'Vivaan', 'Gupta', 'vivaan.gupta@email.com', '9876543212', '2026-08-03 09:15:00'),
(4, 'Ananya', 'Iyer', 'ananya.iyer@email.com', '9876543213', '2026-08-04 14:20:00'),
(5, 'Kabir', 'Singh', 'kabir.singh@email.com', '9876543214', '2026-08-05 16:45:00'),
(6, 'Ishaan', 'Reddy', 'ishaan.reddy@email.com', '9876543215', '2026-08-06 12:10:00'),
(7, 'Myra', 'Nair', 'myra.nair@email.com', '9876543216', '2026-08-07 08:30:00'),
(8, 'Arjun', 'Mehta', 'arjun.mehta@email.com', '9876543217', '2026-08-08 15:55:00'),
(9, 'Kiara', 'Joshi', 'kiara.joshi@email.com', '9876543218', '2026-08-09 11:05:00'),
(10, 'Sai', 'Prasad', 'sai.prasad@email.com', '9876543219', '2026-08-10 13:40:00');

-- 3. Seed Orders (varying dates, amounts, and statuses)
INSERT INTO orders (order_id, customer_id, product_id, quantity, total_amount, order_status, created_at) VALUES
-- Day 1: 2026-08-19
(1001, 1, 1, 1, 1200.00, 'Completed', '2026-08-19 10:15:00'),
(1002, 2, 3, 2, 500.00, 'Completed', '2026-08-19 11:45:00'),
(1003, 3, 4, 1, 150.00, 'Completed', '2026-08-19 14:30:00'),
(1004, 4, 2, 1, 800.00, 'Refunded', '2026-08-19 16:00:00'), -- Refunded order
-- Day 2: 2026-08-20
(1005, 5, 5, 2, 700.00, 'Completed', '2026-08-20 09:30:00'),
(1006, 6, 1, 1, 1200.00, 'Completed', '2026-08-20 12:15:00'),
(1007, 7, 3, 1, 250.00, 'Cancelled', '2026-08-20 15:45:00'), -- Cancelled order
(1008, 8, 2, 1, 800.00, 'Completed', '2026-08-20 17:00:00'),
-- Day 3: 2026-08-21
(1009, 9, 4, 3, 450.00, 'Completed', '2026-08-21 10:00:00'),
(1010, 10, 5, 1, 350.00, 'Completed', '2026-08-21 11:30:00'),
(1011, 1, 2, 1, 800.00, 'Completed', '2026-08-21 13:00:00'),
(1012, 2, 1, 1, 1200.00, 'Completed', '2026-08-21 16:20:00'),
-- Day 4: 2026-08-22
(1013, 3, 3, 1, 250.00, 'Completed', '2026-08-22 09:00:00'),
(1014, 4, 5, 2, 700.00, 'Completed', '2026-08-22 14:10:00'),
(1015, 5, 4, 1, 150.00, 'Refunded', '2026-08-22 15:30:00'); -- Refunded order
