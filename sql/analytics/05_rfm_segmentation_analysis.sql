-- =============================================================================
-- Analytical SQL Query 5: RFM Customer Analytics & Segmentation-Ready Features
--
-- Objective:
-- Computes customer-level RFM metrics directly from the star schema warehouse:
-- 1. Recency (R): Days since most recent Delivered order relative to snapshot date.
-- 2. Frequency (F): Count of distinct Delivered orders placed by customer.
-- 3. Monetary (M): Total recognized revenue spent across Delivered orders.
-- 4. R_Score, F_Score, M_Score: Quartile ranking using NTILE(4).
-- 5. Business Segmentation: Categorizes customers into operational clusters.
-- =============================================================================

USE ecommerce_dwh;

WITH snapshot AS (
    SELECT MAX(d.full_date) AS ref_date FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key
),
customer_rfm_raw AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.email,
        c.city,
        c.state,
        c.signup_date,
        s.ref_date,
        MAX(CASE WHEN f.is_delivered = 1 THEN d.full_date END) AS last_delivered_order_date,
        COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) AS frequency,
        ROUND(COALESCE(SUM(f.recognized_revenue), 0.0), 2) AS monetary,
        ROUND(COALESCE(SUM(f.recognized_profit), 0.0), 2) AS delivered_profit,
        COUNT(DISTINCT CASE WHEN f.is_returned = 1 THEN f.order_id END) AS returned_orders_count,
        COUNT(DISTINCT CASE WHEN f.is_cancelled = 1 THEN f.order_id END) AS cancelled_orders_count,
        COUNT(DISTINCT f.order_id) AS total_orders_placed
    FROM dim_customer c
    CROSS JOIN snapshot s
    LEFT JOIN fact_sales f ON c.customer_key = f.customer_key
    LEFT JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_id, c.customer_name, c.email, c.city, c.state, c.signup_date, s.ref_date
),
customer_rfm_metrics AS (
    SELECT
        customer_id,
        customer_name,
        email,
        city,
        state,
        signup_date,
        last_delivered_order_date,
        frequency,
        monetary,
        delivered_profit,
        returned_orders_count,
        cancelled_orders_count,
        total_orders_placed,
        -- Recency: Days from last delivered order to snapshot (or customer tenure if 0 delivered orders)
        CASE
            WHEN last_delivered_order_date IS NOT NULL THEN DATEDIFF(ref_date, last_delivered_order_date)
            ELSE DATEDIFF(ref_date, signup_date)
        END AS recency_days
    FROM customer_rfm_raw
),
scored_rfm AS (
    SELECT
        *,
        -- For Recency, lower days is better -> reverse quartile: NTILE(4) over DESC gives lowest recency highest score
        NTILE(4) OVER (ORDER BY recency_days DESC) AS r_quartile,
        NTILE(4) OVER (ORDER BY frequency ASC) AS f_quartile,
        NTILE(4) OVER (ORDER BY monetary ASC) AS m_quartile
    FROM customer_rfm_metrics
),
segmented_rfm AS (
    SELECT
        *,
        CASE
            WHEN frequency = 0 THEN 'Inactive / Non-Buyer'
            WHEN r_quartile >= 3 AND f_quartile >= 3 AND m_quartile >= 3 THEN 'Champions'
            WHEN r_quartile >= 3 AND f_quartile >= 2 THEN 'Loyal Customers'
            WHEN r_quartile >= 3 AND f_quartile = 1 THEN 'Recent New Customers'
            WHEN r_quartile <= 2 AND f_quartile >= 3 THEN 'At Risk / Churning'
            WHEN r_quartile <= 2 AND f_quartile <= 2 AND m_quartile >= 2 THEN 'Hibernating / Needs Attention'
            ELSE 'Lost / Low Value'
        END AS rfm_segment
    FROM scored_rfm
)
-- Aggregate Segment Distribution & Value Summary
SELECT
    rfm_segment,
    COUNT(customer_id) AS customer_count,
    ROUND((COUNT(customer_id) * 100.0) / (SELECT COUNT(*) FROM dim_customer), 2) AS customer_share_pct,
    ROUND(AVG(recency_days), 1) AS avg_recency_days,
    ROUND(AVG(frequency), 2) AS avg_frequency,
    ROUND(AVG(monetary), 2) AS avg_monetary,
    ROUND(SUM(monetary), 2) AS total_segment_revenue,
    ROUND(SUM(delivered_profit), 2) AS total_segment_profit
FROM segmented_rfm
GROUP BY rfm_segment
ORDER BY total_segment_revenue DESC;

