-- =============================================================================
-- Analytical SQL Query 4: Customer Lifetime Value (LTV) & Repeat Purchase Analysis
--
-- Objective:
-- Evaluates customer purchasing behavior, repeat order distribution, and LTV.
--
-- Metrics:
-- 1. Total delivered orders and total delivered spend per customer.
-- 2. Classification into One-Time Buyers vs Repeat Buyers (2, 3, 4+ orders).
-- 3. Customer Lifetime Value (CLV/LTV) deciles.
-- 4. Overall cohort summary of repeat purchase contribution.
-- =============================================================================

USE ecommerce_dwh;

-- 1. Customer-Level LTV Summary with Purchase Cohort Classification
WITH customer_purchases AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.city,
        c.state,
        c.signup_date,
        COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) AS delivered_orders_count,
        COUNT(DISTINCT f.order_id) AS total_orders_placed,
        ROUND(SUM(f.recognized_revenue), 2) AS customer_ltv,
        ROUND(SUM(f.recognized_profit), 2) AS customer_profit_contribution,
        CASE
            WHEN COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) > 0 THEN
                ROUND(SUM(f.recognized_revenue) / COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END), 2)
            ELSE 0.00
        END AS avg_order_value,
        MIN(CASE WHEN f.is_delivered = 1 THEN d.full_date END) AS first_order_date,
        MAX(CASE WHEN f.is_delivered = 1 THEN d.full_date END) AS latest_order_date
    FROM dim_customer c
    LEFT JOIN fact_sales f ON c.customer_key = f.customer_key
    LEFT JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_id, c.customer_name, c.city, c.state, c.signup_date
),
categorized_customers AS (
    SELECT
        customer_id,
        customer_name,
        city,
        state,
        signup_date,
        delivered_orders_count,
        total_orders_placed,
        customer_ltv,
        customer_profit_contribution,
        avg_order_value,
        first_order_date,
        latest_order_date,
        CASE
            WHEN delivered_orders_count = 0 THEN 'Zero Delivered Orders'
            WHEN delivered_orders_count = 1 THEN 'One-Time Buyer'
            WHEN delivered_orders_count = 2 THEN '2-Time Repeat Buyer'
            WHEN delivered_orders_count = 3 THEN '3-Time Repeat Buyer'
            ELSE 'High-Frequency Buyer (4+ Orders)'
        END AS buyer_cohort
    FROM customer_purchases
)
-- Repeat Purchase Summary Table
SELECT
    buyer_cohort,
    COUNT(customer_id) AS customer_count,
    ROUND((COUNT(customer_id) * 100.0) / (SELECT COUNT(*) FROM dim_customer), 2) AS customer_share_pct,
    SUM(delivered_orders_count) AS total_delivered_orders,
    ROUND(SUM(customer_ltv), 2) AS total_recognized_revenue,
    CASE
        WHEN (SELECT SUM(recognized_revenue) FROM fact_sales) > 0 THEN
            ROUND((SUM(customer_ltv) * 100.0) / (SELECT SUM(recognized_revenue) FROM fact_sales), 2)
        ELSE 0.00
    END AS revenue_share_pct,
    ROUND(AVG(customer_ltv), 2) AS avg_clv_per_customer,
    ROUND(AVG(avg_order_value), 2) AS avg_aov
FROM categorized_customers
GROUP BY buyer_cohort
ORDER BY total_recognized_revenue DESC;

