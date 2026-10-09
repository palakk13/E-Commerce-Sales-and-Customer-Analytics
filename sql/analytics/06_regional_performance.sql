-- =============================================================================
-- Analytical SQL Query 6: Regional and Location-Wise Performance
--
-- Objective:
-- Analyzes sales distribution, recognized revenue, and profit across geographic
-- regions (Country, State, and City) using dim_location.
--
-- Evaluates regional gross margin % and identifies top revenue-generating markets.
-- =============================================================================

USE ecommerce_dwh;

-- 1. State-Level Performance Summary
SELECT
    l.state,
    l.country,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    COUNT(DISTINCT f.order_id) AS total_orders_placed,
    COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) AS delivered_orders,
    SUM(f.quantity) AS total_units_ordered,
    SUM(CASE WHEN f.is_delivered = 1 THEN f.quantity ELSE 0 END) AS units_delivered,
    ROUND(SUM(f.gross_revenue), 2) AS gross_sales_demand,
    ROUND(SUM(f.recognized_revenue), 2) AS recognized_revenue,
    ROUND(SUM(f.recognized_profit), 2) AS recognized_profit,
    CASE
        WHEN SUM(f.recognized_revenue) > 0 THEN
            ROUND((SUM(f.recognized_profit) / SUM(f.recognized_revenue)) * 100.0, 2)
        ELSE 0.00
    END AS regional_margin_pct,
    DENSE_RANK() OVER (ORDER BY SUM(f.recognized_revenue) DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY SUM(f.recognized_profit) DESC) AS profit_rank
FROM fact_sales f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.state, l.country
ORDER BY revenue_rank ASC;

-- 2. Top 15 Metros / Cities by Delivered Revenue
SELECT
    l.city,
    l.state,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) AS delivered_orders,
    ROUND(SUM(f.recognized_revenue), 2) AS recognized_revenue,
    ROUND(SUM(f.recognized_profit), 2) AS recognized_profit,
    ROUND(AVG(CASE WHEN f.is_delivered = 1 THEN f.net_revenue END), 2) AS avg_item_net_revenue
FROM fact_sales f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.city, l.state
ORDER BY recognized_revenue DESC
LIMIT 15;
