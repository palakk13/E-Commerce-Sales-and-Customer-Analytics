-- =============================================================================
-- E-Commerce Sales and Customer Analytics Platform
-- Master Warehouse Initialization & Teardown Script
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ecommerce_dwh
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ecommerce_dwh;

-- Drop existing tables in reverse dependency order
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_payment;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_date;
SET FOREIGN_KEY_CHECKS = 1;

-- 1. dim_date
CREATE TABLE dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    day_of_week TINYINT NOT NULL COMMENT '1=Monday through 7=Sunday',
    day_name VARCHAR(15) NOT NULL,
    day_of_month TINYINT NOT NULL,
    month_num TINYINT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter TINYINT NOT NULL,
    year SMALLINT NOT NULL,
    is_weekend TINYINT NOT NULL DEFAULT 0 COMMENT '1=Weekend, 0=Weekday',
    PRIMARY KEY (date_key),
    UNIQUE KEY uq_dim_date_full_date (full_date),
    INDEX idx_dim_date_year_month (year, month_num)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. dim_location
CREATE TABLE dim_location (
    location_key INT NOT NULL AUTO_INCREMENT,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (location_key),
    UNIQUE KEY uq_dim_location_geo (city, state, country, postal_code),
    INDEX idx_dim_location_state (state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. dim_customer
CREATE TABLE dim_customer (
    customer_key INT NOT NULL AUTO_INCREMENT,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(150) NOT NULL,
    email VARCHAR(150) NOT NULL,
    phone VARCHAR(50) NULL DEFAULT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    signup_date DATE NOT NULL,
    rfm_segment VARCHAR(50) NULL DEFAULT 'Unassigned',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_key),
    UNIQUE KEY uq_dim_customer_id (customer_id),
    INDEX idx_dim_customer_state (state),
    INDEX idx_dim_customer_segment (rfm_segment)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. dim_product
CREATE TABLE dim_product (
    product_key INT NOT NULL AUTO_INCREMENT,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(100) NOT NULL,
    sub_category VARCHAR(100) NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    list_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_key),
    UNIQUE KEY uq_dim_product_id (product_id),
    INDEX idx_dim_product_category (category, sub_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. dim_payment
CREATE TABLE dim_payment (
    payment_key INT NOT NULL AUTO_INCREMENT,
    payment_method VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (payment_key),
    UNIQUE KEY uq_dim_payment_method (payment_method)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6. fact_sales
CREATE TABLE fact_sales (
    sales_key BIGINT NOT NULL AUTO_INCREMENT,
    order_id VARCHAR(50) NOT NULL,
    order_item_id VARCHAR(50) NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    location_key INT NOT NULL,
    payment_key INT NOT NULL,
    order_status VARCHAR(30) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) NOT NULL,
    gross_revenue DECIMAL(12,2) NOT NULL,
    discount_amount DECIMAL(12,2) NOT NULL,
    net_revenue DECIMAL(12,2) NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    total_cost DECIMAL(12,2) NOT NULL,
    profit DECIMAL(12,2) NOT NULL,
    profit_margin_pct DECIMAL(5,2) NOT NULL,
    shipping_cost DECIMAL(10,2) NOT NULL,
    is_delivered TINYINT NOT NULL DEFAULT 0,
    is_shipped TINYINT NOT NULL DEFAULT 0,
    is_processing TINYINT NOT NULL DEFAULT 0,
    is_returned TINYINT NOT NULL DEFAULT 0,
    is_cancelled TINYINT NOT NULL DEFAULT 0,
    recognized_revenue DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    recognized_profit DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    in_transit_revenue DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    pipeline_revenue DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    returned_revenue_loss DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    cancelled_revenue_loss DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    customer_order_rank INT NOT NULL DEFAULT 1,
    customer_cumulative_spend DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sales_key),
    UNIQUE KEY uq_fact_sales_order_item (order_item_id),
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_key) REFERENCES dim_product (product_key),
    CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_sales_location FOREIGN KEY (location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_fact_sales_payment FOREIGN KEY (payment_key) REFERENCES dim_payment (payment_key),
    INDEX idx_fact_sales_customer (customer_key),
    INDEX idx_fact_sales_product (product_key),
    INDEX idx_fact_sales_date (date_key),
    INDEX idx_fact_sales_location (location_key),
    INDEX idx_fact_sales_payment (payment_key),
    INDEX idx_fact_sales_status (order_status),
    INDEX idx_fact_sales_order_id (order_id),
    INDEX idx_fact_sales_date_customer (date_key, customer_key),
    INDEX idx_fact_sales_product_date (product_key, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
