-- =============================================================================
-- E-Commerce Sales and Customer Analytics Platform
-- Dimension Tables DDL
-- =============================================================================

USE ecommerce_dwh;

-- -----------------------------------------------------------------------------
-- 1. Date Dimension (Conformed Calendar Dimension)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
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

-- -----------------------------------------------------------------------------
-- 2. Geographic Location Dimension
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_location (
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

-- -----------------------------------------------------------------------------
-- 3. Customer Profile Dimension
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer (
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

-- -----------------------------------------------------------------------------
-- 4. Product Merchandising Dimension
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
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

-- -----------------------------------------------------------------------------
-- 5. Payment Method Dimension
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_payment (
    payment_key INT NOT NULL AUTO_INCREMENT,
    payment_method VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (payment_key),
    UNIQUE KEY uq_dim_payment_method (payment_method)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
