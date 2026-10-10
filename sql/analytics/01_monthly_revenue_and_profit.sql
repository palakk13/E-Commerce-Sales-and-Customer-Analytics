-- =============================================================================
-- Analytical SQL Query 1: Monthly Financial Performance
--
-- Objective:
-- Computes monthly gross demand, discounts, net demand, delivered recognized revenue,
-- delivered COGS, delivered recognized profit, and operating profit margin %.
--
-- Accounting Rule:
-- Enforces strict FOB Destination revenue recognition where recognized revenue and
-- profit are derived exclusively from orders with status 'Delivered'.
-- Non-delivered orders are tracked in operational pipeline/loss columns.
-- =============================================================================

USE ecommerce_dwh;

WITH monthly_sales AS (
    SELECT
        d.year,
        d.month_num,
        d.month_name,
        CONCAT(d.year, '-', LPAD(d.month_num, 2, '0')) AS year_month,
        
        -- Order volume metrics
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) AS delivered_orders,
        COUNT(DISTINCT f.customer_key) AS unique_customers,
        SUM(f.quantity) AS total_units_ordered,
        SUM(CASE WHEN f.is_delivered = 1 THEN f.quantity ELSE 0 END) AS units_delivered,
        
        -- Top-line Demand measures
        ROUND(SUM(f.gross_revenue), 2) AS gross_sales_demand,
        ROUND(SUM(f.discount_amount), 2) AS discount_amount,
        ROUND(SUM(f.net_revenue), 2) AS net_sales_demand,
        
        -- Realized Financials (Delivered Only)
        ROUND(SUM(f.recognized_revenue), 2) AS recognized_revenue,
        ROUND(SUM(CASE WHEN f.is_delivered = 1 THEN f.total_cost ELSE 0 END), 2) AS recognized_cogs,
        ROUND(SUM(f.recognized_profit), 2) AS recognized_profit,
        
        -- Operational Pipeline & Attrition Breakdown
        ROUND(SUM(f.in_transit_revenue), 2) AS in_transit_revenue,
        ROUND(SUM(f.pipeline_revenue), 2) AS pipeline_revenue,
        ROUND(SUM(f.returned_revenue_loss), 2) AS returned_revenue_loss,
        ROUND(SUM(f.cancelled_revenue_loss), 2) AS cancelled_revenue_loss
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month_num, d.month_name, CONCAT(d.year, '-', LPAD(d.month_num, 2, '0'))
)
SELECT
    year_month,
    year,
    month_name,
    total_orders,
    delivered_orders,
    unique_customers,
    total_units_ordered,
    units_delivered,
    gross_sales_demand,
    discount_amount,
    net_sales_demand,
    recognized_revenue,
    recognized_cogs,
    recognized_profit,
    CASE
        WHEN recognized_revenue > 0 THEN ROUND((recognized_profit / recognized_revenue) * 100.0, 2)
        ELSE 0.00
    END AS recognized_margin_pct,
    in_transit_revenue,
    pipeline_revenue,
    returned_revenue_loss,
    cancelled_revenue_loss
FROM monthly_sales
ORDER BY year_month ASC;
