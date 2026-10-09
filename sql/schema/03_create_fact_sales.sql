-- =============================================================================
-- E-Commerce Sales and Customer Analytics Platform
-- Fact Table DDL (Atomic Line-Item Grain)
-- =============================================================================

USE ecommerce_dwh;

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key BIGINT NOT NULL AUTO_INCREMENT,
    
    -- Degenerate Dimensions / Transaction Identifiers
    order_id VARCHAR(50) NOT NULL,
    order_item_id VARCHAR(50) NOT NULL,
    
    -- Dimension Foreign Keys
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    location_key INT NOT NULL,
    payment_key INT NOT NULL,
    
    -- Operational Status
    order_status VARCHAR(30) NOT NULL,
    
    -- Pricing and Volume Measures
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
    
    -- Fulfillment Indicators
    is_delivered TINYINT NOT NULL DEFAULT 0,
    is_shipped TINYINT NOT NULL DEFAULT 0,
    is_processing TINYINT NOT NULL DEFAULT 0,
    is_returned TINYINT NOT NULL DEFAULT 0,
    is_cancelled TINYINT NOT NULL DEFAULT 0,
    
    -- Realized Financials (FOB Destination Rule: Delivered Orders Only)
    recognized_revenue DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    recognized_profit DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    in_transit_revenue DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    pipeline_revenue DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    returned_revenue_loss DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    cancelled_revenue_loss DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    
    -- Window Sequence Measures
    customer_order_rank INT NOT NULL DEFAULT 1,
    customer_cumulative_spend DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    
    -- Audit Metadata
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (sales_key),
    UNIQUE KEY uq_fact_sales_order_item (order_item_id),
    
    -- Foreign Key Constraints
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_key) REFERENCES dim_product (product_key),
    CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_sales_location FOREIGN KEY (location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_fact_sales_payment FOREIGN KEY (payment_key) REFERENCES dim_payment (payment_key),
    
    -- Query Optimization Indexes
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
