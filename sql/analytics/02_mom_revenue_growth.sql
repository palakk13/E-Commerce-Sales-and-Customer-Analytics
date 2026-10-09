-- =============================================================================
-- Analytical SQL Query 2: Month-over-Month Revenue Growth & Running Totals
--
-- Objective:
-- Computes time-series trajectory of recognized revenue:
-- 1. Monthly recognized revenue.
-- 2. Running total cumulative recognized revenue (Window Function).
-- 3. Prior-month recognized revenue (LAG Window Function).
-- 4. Month-over-Month (MoM) revenue growth percentage.
-- =============================================================================

USE ecommerce_dwh;

WITH monthly_revenue AS (
    SELECT
        CONCAT(d.year, '-', LPAD(d.month_num, 2, '0')) AS year_month,
        d.year,
        d.month_num,
        ROUND(SUM(f.recognized_revenue), 2) AS monthly_recognized_revenue,
        ROUND(SUM(f.recognized_profit), 2) AS monthly_recognized_profit,
        COUNT(DISTINCT CASE WHEN f.is_delivered = 1 THEN f.order_id END) AS delivered_orders
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month_num, CONCAT(d.year, '-', LPAD(d.month_num, 2, '0'))
),
windowed_revenue AS (
    SELECT
        year_month,
        year,
        month_num,
        monthly_recognized_revenue,
        monthly_recognized_profit,
        delivered_orders,
        -- Running cumulative recognized revenue
        ROUND(SUM(monthly_recognized_revenue) OVER (
            ORDER BY year_month ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2) AS running_total_recognized_revenue,
        -- Prior month revenue via LAG
        LAG(monthly_recognized_revenue, 1) OVER (
            ORDER BY year_month ASC
        ) AS prev_month_recognized_revenue
    FROM monthly_revenue
)
SELECT
    year_month,
    delivered_orders,
    monthly_recognized_revenue,
    running_total_recognized_revenue,
    prev_month_recognized_revenue,
    CASE
        WHEN prev_month_recognized_revenue IS NOT NULL AND prev_month_recognized_revenue > 0 THEN
            ROUND(((monthly_recognized_revenue - prev_month_recognized_revenue) / prev_month_recognized_revenue) * 100.0, 2)
        ELSE 0.00
    END AS mom_growth_pct,
    monthly_recognized_profit
FROM windowed_revenue
ORDER BY year_month ASC;
