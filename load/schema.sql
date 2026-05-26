-- Dimension: customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id       VARCHAR(50) PRIMARY KEY,
    customer_city     VARCHAR(100),
    customer_state    VARCHAR(10)
);

-- Dimension: products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id            VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100)
);

-- Fact: orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id              VARCHAR(50) PRIMARY KEY,
    customer_id           VARCHAR(50),
    order_date            DATE,
    order_status          VARCHAR(30),
    payment_value         NUMERIC(10,2),
    delivery_days         INT,
    is_late               SMALLINT,
    product_category_name VARCHAR(100),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);

-- Aggregate: daily revenue
CREATE TABLE IF NOT EXISTS daily_revenue (
    order_date    DATE PRIMARY KEY,
    total_revenue NUMERIC(12,2),
    total_orders  INT
);

-- Aggregate: category revenue
CREATE TABLE IF NOT EXISTS category_revenue (
    product_category_name VARCHAR(100) PRIMARY KEY,
    total_revenue         NUMERIC(12,2),
    total_orders          INT,
    avg_order_value       NUMERIC(10,2)
);