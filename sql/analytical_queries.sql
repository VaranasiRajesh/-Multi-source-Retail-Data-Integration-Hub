-- ═══════════════════════════════════════════════════════════════════════
-- Analytical Queries for the DataFoundation Retail Data Warehouse
-- ═══════════════════════════════════════════════════════════════════════


-- ─── 1. Monthly Sales Trend ─────────────────────────────────────────
-- Revenue, transaction count, and growth by month
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.total_amount) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    SUM(f.quantity) AS total_units_sold,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    COUNT(DISTINCT f.customer_id) AS unique_customers,
    LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month) AS prev_month_revenue,
    ROUND(
        (SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month))
        / NULLIF(LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month), 0) * 100,
        2
    ) AS mom_growth_pct
FROM `multi-source-retail-data.retail_dw.fact_sales` f
JOIN `multi-source-retail-data.retail_dw.dim_date` d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ─── 2. Category Performance Comparison ─────────────────────────────
-- Revenue and customer metrics by product category
SELECT
    f.product_category,
    pc.category_group,
    SUM(f.total_amount) AS total_revenue,
    ROUND(SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER () * 100, 2) AS revenue_share_pct,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    SUM(f.quantity) AS total_quantity,
    ROUND(AVG(f.price_per_unit), 2) AS avg_unit_price,
    COUNT(DISTINCT f.customer_id) AS unique_customers,
    ROUND(AVG(f.age), 1) AS avg_customer_age
FROM `multi-source-retail-data.retail_dw.fact_sales` f
LEFT JOIN `multi-source-retail-data.retail_dw.dim_product_category` pc 
    ON f.product_category = pc.category_name
GROUP BY f.product_category, pc.category_group
ORDER BY total_revenue DESC;


-- ─── 3. Customer Segmentation Analysis ──────────────────────────────
-- Revenue and demographics by customer segment
SELECT
    c.customer_segment,
    c.age_group,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    SUM(f.quantity) AS total_quantity,
    ROUND(AVG(c.total_transactions), 1) AS avg_purchases_per_customer
FROM `multi-source-retail-data.retail_dw.fact_sales` f
JOIN `multi-source-retail-data.retail_dw.dim_customer` c 
    ON f.customer_key = c.customer_key AND c.is_current = TRUE
GROUP BY c.customer_segment, c.age_group
ORDER BY c.customer_segment, c.age_group;


-- ─── 4. Gender-based Sales Analysis ─────────────────────────────────
SELECT
    f.gender,
    f.product_category,
    SUM(f.total_amount) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    ROUND(AVG(f.age), 1) AS avg_age
FROM `multi-source-retail-data.retail_dw.fact_sales` f
GROUP BY f.gender, f.product_category
ORDER BY f.gender, total_revenue DESC;


-- ─── 5. Weekend vs Weekday Sales ────────────────────────────────────
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    d.day_name,
    SUM(f.total_amount) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value
FROM `multi-source-retail-data.retail_dw.fact_sales` f
JOIN `multi-source-retail-data.retail_dw.dim_date` d ON f.date_key = d.date_key
GROUP BY day_type, d.day_name, d.day_of_week
ORDER BY d.day_of_week;


-- ─── 6. Top Customers by Revenue ────────────────────────────────────
SELECT
    c.customer_id,
    c.gender,
    c.age,
    c.age_group,
    c.customer_segment,
    c.total_transactions,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.quantity) AS total_quantity,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value
FROM `multi-source-retail-data.retail_dw.fact_sales` f
JOIN `multi-source-retail-data.retail_dw.dim_customer` c 
    ON f.customer_key = c.customer_key AND c.is_current = TRUE
GROUP BY c.customer_id, c.gender, c.age, c.age_group, 
         c.customer_segment, c.total_transactions
ORDER BY total_revenue DESC
LIMIT 20;


-- ─── 7. Product Catalog Overview (from API) ─────────────────────────
SELECT
    p.product_category,
    COUNT(*) AS product_count,
    ROUND(AVG(p.api_price), 2) AS avg_price,
    ROUND(MIN(p.api_price), 2) AS min_price,
    ROUND(MAX(p.api_price), 2) AS max_price,
    ROUND(AVG(p.rating_rate), 2) AS avg_rating,
    SUM(p.rating_count) AS total_reviews
FROM `multi-source-retail-data.retail_dw.dim_product` p
WHERE p.is_current = TRUE
GROUP BY p.product_category
ORDER BY product_count DESC;


-- ─── 8. Quarterly Trend with YoY Comparison ─────────────────────────
SELECT
    d.year,
    d.quarter,
    SUM(f.total_amount) AS quarterly_revenue,
    COUNT(DISTINCT f.transaction_id) AS quarterly_transactions,
    COUNT(DISTINCT f.customer_id) AS quarterly_customers,
    LAG(SUM(f.total_amount), 4) OVER (ORDER BY d.year, d.quarter) AS same_quarter_prev_year,
    ROUND(
        (SUM(f.total_amount) - LAG(SUM(f.total_amount), 4) OVER (ORDER BY d.year, d.quarter))
        / NULLIF(LAG(SUM(f.total_amount), 4) OVER (ORDER BY d.year, d.quarter), 0) * 100,
        2
    ) AS yoy_growth_pct
FROM `multi-source-retail-data.retail_dw.fact_sales` f
JOIN `multi-source-retail-data.retail_dw.dim_date` d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;
