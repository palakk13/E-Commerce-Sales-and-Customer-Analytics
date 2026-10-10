-- =============================================================================
-- Analytical SQL Query 3: Product and Category Merchandising Rankings
--
-- Objective:
-- Ranks categories, sub-categories, and individual products by delivered profit,
-- recognized revenue, and sales volume.
--
-- Uses DENSE_RANK() window functions to evaluate top-performing merchandise.
-- =============================================================================

USE ecommerce_dwh;

-- 1. Category & Sub-Category Performance Ranking
SELECT
    p.category,
    p.sub_category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) AS delivered_orders,
    SUM(f.quantity) AS total_units_ordered,
    SUM(CASE WHEN f.is_delivered = 1 THEN f.quantity ELSE 0 END) AS units_delivered,
    ROUND(SUM(f.gross_revenue), 2) AS gross_sales_demand,
    ROUND(SUM(f.recognized_revenue), 2) AS recognized_revenue,
    ROUND(SUM(CASE WHEN f.is_delivered = 1 THEN f.total_cost ELSE 0 END), 2) AS delivered_cogs,
    ROUND(SUM(f.recognized_profit), 2) AS recognized_profit,
    CASE
        WHEN SUM(f.recognized_revenue) > 0 THEN
            ROUND((SUM(f.recognized_profit) / SUM(f.recognized_revenue)) * 100.0, 2)
        ELSE 0.00
    END AS delivered_margin_pct,
    DENSE_RANK() OVER (ORDER BY SUM(f.recognized_profit) DESC) AS category_profit_rank,
    DENSE_RANK() OVER (ORDER BY SUM(f.recognized_revenue) DESC) AS category_revenue_rank
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category, p.sub_category
ORDER BY category_profit_rank ASC;

-- 2. Top 10 Product SKUs by Recognized Profit
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.unit_cost,
    p.list_price,
    SUM(CASE WHEN f.is_delivered = 1 THEN f.quantity ELSE 0 END) AS units_delivered,
    ROUND(SUM(f.recognized_revenue), 2) AS recognized_revenue,
    ROUND(SUM(f.recognized_profit), 2) AS recognized_profit,
    CASE
        WHEN SUM(f.recognized_revenue) > 0 THEN
            ROUND((SUM(f.recognized_profit) / SUM(f.recognized_revenue)) * 100.0, 2)
        ELSE 0.00
    END AS product_margin_pct,
    DENSE_RANK() OVER (ORDER BY SUM(f.recognized_profit) DESC) AS product_profit_rank
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_id, p.product_name, p.category, p.sub_category, p.unit_cost, p.list_price
ORDER BY product_profit_rank ASC
LIMIT 10;
