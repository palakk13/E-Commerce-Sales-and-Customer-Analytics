-- =============================================================================
-- Analytical SQL Query 7: Payment-Method Adoption & Order Fulfillment Funnel
--
-- Objective:
-- 1. Evaluates payment channel preference, order volume, and recognized revenue.
-- 2. Analyzes the order status fulfillment funnel (Delivered vs Shipped vs Processing
--    vs Cancelled vs Returned).
-- 3. Calculates operational attrition rates (Cancellation Rate % and Return Rate %).
-- =============================================================================

USE ecommerce_dwh;

-- 1. Payment Method Performance Breakdown
SELECT
    p.payment_method,
    COUNT(DISTINCT f.order_id) AS total_orders_placed,
    COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) AS delivered_orders,
    ROUND((COUNT(DISTINCT f.order_id) * 100.0) / (SELECT COUNT(DISTINCT order_id) FROM fact_sales), 2) AS order_share_pct,
    ROUND(SUM(f.gross_revenue), 2) AS gross_sales_demand,
    ROUND(SUM(f.net_revenue), 2) AS net_sales_demand,
    ROUND(SUM(f.recognized_revenue), 2) AS recognized_revenue,
    ROUND(SUM(f.recognized_profit), 2) AS recognized_profit,
    ROUND(SUM(f.cancelled_revenue_loss), 2) AS cancelled_loss,
    ROUND(SUM(f.returned_revenue_loss), 2) AS returned_loss
FROM fact_sales f
JOIN dim_payment p ON f.payment_key = p.payment_key
GROUP BY p.payment_method
ORDER BY total_orders_placed DESC;

-- 2. Order Fulfillment Funnel & Attrition Rates
WITH status_summary AS (
    SELECT
        f.order_status,
        COUNT(f.order_item_id) AS line_items_count,
        COUNT(DISTINCT f.order_id) AS distinct_orders,
        ROUND(SUM(f.gross_revenue), 2) AS gross_revenue,
        ROUND(SUM(f.discount_amount), 2) AS discount_amount,
        ROUND(SUM(f.net_revenue), 2) AS net_revenue,
        ROUND(SUM(f.recognized_revenue), 2) AS recognized_revenue,
        ROUND(SUM(f.recognized_profit), 2) AS recognized_profit,
        ROUND(SUM(f.in_transit_revenue), 2) AS in_transit_revenue,
        ROUND(SUM(f.pipeline_revenue), 2) AS pipeline_revenue,
        ROUND(SUM(f.returned_revenue_loss), 2) AS returned_revenue_loss,
        ROUND(SUM(f.cancelled_revenue_loss), 2) AS cancelled_revenue_loss
    FROM fact_sales f
    GROUP BY f.order_status
)
SELECT
    order_status,
    line_items_count,
    distinct_orders,
    ROUND((distinct_orders * 100.0) / (SELECT SUM(distinct_orders) FROM status_summary), 2) AS order_percentage,
    gross_revenue,
    net_revenue,
    recognized_revenue,
    recognized_profit,
    in_transit_revenue,
    pipeline_revenue,
    returned_revenue_loss,
    cancelled_revenue_loss
FROM status_summary
ORDER BY distinct_orders DESC;

