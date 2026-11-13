-- ============================================================================
-- RETAIL MESHJOIN DATA WAREHOUSE — OLAP ANALYTICAL QUERIES (Q1 - Q20)
-- Database: retail_dw
-- Engine: MySQL 8.0+
-- All queries use the pre-computed and stored `revenue` column from FACT_SALES.
-- ============================================================================

USE retail_dw;

-- ----------------------------------------------------------------------------
-- Q1: Top 5 products by total revenue for the year, split by weekday/weekend,
--     with monthly drill-down for those same 5 products.
-- ----------------------------------------------------------------------------
WITH Top5Products AS (
    SELECT 
        f.product_key,
        p.product_id,
        p.product_name,
        SUM(f.revenue) AS total_year_revenue
    FROM FACT_SALES f
    JOIN DIM_PRODUCT p ON f.product_key = p.product_key
    JOIN DIM_TIME t ON f.time_key = t.time_key
    GROUP BY f.product_key, p.product_id, p.product_name
    ORDER BY total_year_revenue DESC
    LIMIT 5
)
SELECT 
    tp.product_id,
    tp.product_name,
    t.year,
    t.month,
    CASE WHEN t.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.revenue) AS monthly_revenue
FROM FACT_SALES f
JOIN Top5Products tp ON f.product_key = tp.product_key
JOIN DIM_TIME t ON f.time_key = t.time_key
GROUP BY tp.product_id, tp.product_name, t.year, t.month, t.is_weekend
ORDER BY tp.product_id, t.year, t.month, day_type;


-- ----------------------------------------------------------------------------
-- Q2: Total revenue grouped by customer demographic attributes
--     (gender, age_group, city_category).
-- ----------------------------------------------------------------------------
SELECT 
    c.gender,
    c.age_group,
    c.city_category,
    SUM(f.revenue) AS total_revenue
FROM FACT_SALES f
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
GROUP BY c.gender, c.age_group, c.city_category
ORDER BY c.gender, c.age_group, c.city_category;


-- ----------------------------------------------------------------------------
-- Q3: Total revenue grouped by product category and customer occupation.
-- ----------------------------------------------------------------------------
SELECT 
    p.product_category,
    c.occupation,
    SUM(f.revenue) AS total_revenue
FROM FACT_SALES f
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
GROUP BY p.product_category, c.occupation
ORDER BY p.product_category, total_revenue DESC;


-- ----------------------------------------------------------------------------
-- Q4: Total revenue grouped by gender, age_group, year, and quarter.
-- ----------------------------------------------------------------------------
SELECT 
    c.gender,
    c.age_group,
    t.year,
    t.quarter,
    SUM(f.revenue) AS total_revenue
FROM FACT_SALES f
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN DIM_TIME t ON f.time_key = t.time_key
GROUP BY c.gender, c.age_group, t.year, t.quarter
ORDER BY c.gender, c.age_group, t.year, t.quarter;


-- ----------------------------------------------------------------------------
-- Q5: Top 5 customer occupations generating highest revenue within each product category.
-- ----------------------------------------------------------------------------
WITH RankedOccupations AS (
    SELECT 
        p.product_category,
        c.occupation,
        SUM(f.revenue) AS category_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY p.product_category 
            ORDER BY SUM(f.revenue) DESC
        ) AS occ_rank
    FROM FACT_SALES f
    JOIN DIM_PRODUCT p ON f.product_key = p.product_key
    JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
    GROUP BY p.product_category, c.occupation
)
SELECT 
    product_category,
    occupation,
    category_revenue
FROM RankedOccupations
WHERE occ_rank <= 5
ORDER BY product_category, occ_rank;


-- ----------------------------------------------------------------------------
-- Q6: Total revenue by city category, marital status, and month for the last 6 months
--     (July 2011 to December 2011).
-- ----------------------------------------------------------------------------
SELECT 
    c.city_category,
    c.marital_status,
    t.year,
    t.month,
    SUM(f.revenue) AS total_revenue
FROM FACT_SALES f
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN DIM_TIME t ON f.time_key = t.time_key
WHERE t.event_date >= '2011-07-01' AND t.event_date <= '2011-12-31'
GROUP BY c.city_category, c.marital_status, t.year, t.month
ORDER BY c.city_category, c.marital_status, t.year, t.month;


-- ----------------------------------------------------------------------------
-- Q7: Average revenue per transaction grouped by customer stay duration in current city and gender.
-- ----------------------------------------------------------------------------
SELECT 
    c.stay_in_current_city_years,
    c.gender,
    ROUND(AVG(f.revenue), 2) AS avg_revenue
FROM FACT_SALES f
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
GROUP BY c.stay_in_current_city_years, c.gender
ORDER BY c.stay_in_current_city_years, c.gender;


-- ----------------------------------------------------------------------------
-- Q8: Total revenue by product category and city category, with ranking within each product category.
-- ----------------------------------------------------------------------------
SELECT 
    p.product_category,
    c.city_category,
    SUM(f.revenue) AS total_revenue,
    RANK() OVER (
        PARTITION BY p.product_category 
        ORDER BY SUM(f.revenue) DESC
    ) AS category_rank
FROM FACT_SALES f
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
GROUP BY p.product_category, c.city_category
ORDER BY p.product_category, category_rank;


-- ----------------------------------------------------------------------------
-- Q9: Month-over-month revenue growth rate (%) per product category. Uses LAG() with NULL for initial month.
-- ----------------------------------------------------------------------------
WITH MonthlyCategoryRevenue AS (
    SELECT 
        p.product_category,
        t.year,
        t.month,
        SUM(f.revenue) AS revenue
    FROM FACT_SALES f
    JOIN DIM_PRODUCT p ON f.product_key = p.product_key
    JOIN DIM_TIME t ON f.time_key = t.time_key
    GROUP BY p.product_category, t.year, t.month
)
SELECT 
    product_category,
    year,
    month,
    revenue AS current_month_revenue,
    LAG(revenue, 1) OVER (
        PARTITION BY product_category 
        ORDER BY year, month
    ) AS previous_month_revenue,
    CASE 
        WHEN LAG(revenue, 1) OVER (PARTITION BY product_category ORDER BY year, month) IS NULL THEN NULL
        WHEN LAG(revenue, 1) OVER (PARTITION BY product_category ORDER BY year, month) = 0 THEN NULL
        ELSE ROUND(
            ((revenue - LAG(revenue, 1) OVER (PARTITION BY product_category ORDER BY year, month)) 
             / LAG(revenue, 1) OVER (PARTITION BY product_category ORDER BY year, month)) * 100, 
            2
        )
    END AS mom_growth_percent
FROM MonthlyCategoryRevenue
ORDER BY product_category, year, month;


-- ----------------------------------------------------------------------------
-- Q10: Total revenue by customer age group split by weekday vs. weekend.
-- ----------------------------------------------------------------------------
SELECT 
    c.age_group,
    CASE WHEN t.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.revenue) AS total_revenue
FROM FACT_SALES f
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN DIM_TIME t ON f.time_key = t.time_key
GROUP BY c.age_group, t.is_weekend
ORDER BY c.age_group, day_type;


-- ----------------------------------------------------------------------------
-- Q11: Top 5 products per month split by weekday and weekend. Distinct from Q1 (per-month ranking).
-- ----------------------------------------------------------------------------
WITH MonthlyProductRevenue AS (
    SELECT 
        t.year,
        t.month,
        CASE WHEN t.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        p.product_id,
        p.product_name,
        SUM(f.revenue) AS total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY t.year, t.month, t.is_weekend 
            ORDER BY SUM(f.revenue) DESC
        ) AS prod_rank
    FROM FACT_SALES f
    JOIN DIM_PRODUCT p ON f.product_key = p.product_key
    JOIN DIM_TIME t ON f.time_key = t.time_key
    GROUP BY t.year, t.month, t.is_weekend, p.product_id, p.product_name
)
SELECT 
    year,
    month,
    day_type,
    prod_rank,
    product_id,
    product_name,
    total_revenue
FROM MonthlyProductRevenue
WHERE prod_rank <= 5
ORDER BY year, month, day_type, prod_rank;


-- ----------------------------------------------------------------------------
-- Q12: Quarter-over-quarter revenue growth rate (%) per store. Uses LAG() with NULL for initial quarter.
-- ----------------------------------------------------------------------------
WITH QuarterlyStoreRevenue AS (
    SELECT 
        s.store_id,
        s.store_name,
        t.year,
        t.quarter,
        SUM(f.revenue) AS revenue
    FROM FACT_SALES f
    JOIN DIM_STORE s ON f.store_key = s.store_key
    JOIN DIM_TIME t ON f.time_key = t.time_key
    GROUP BY s.store_id, s.store_name, t.year, t.quarter
)
SELECT 
    store_name,
    year,
    quarter,
    revenue AS current_quarter_revenue,
    LAG(revenue, 1) OVER (
        PARTITION BY store_id 
        ORDER BY year, quarter
    ) AS prev_quarter_revenue,
    CASE 
        WHEN LAG(revenue, 1) OVER (PARTITION BY store_id ORDER BY year, quarter) IS NULL THEN NULL
        WHEN LAG(revenue, 1) OVER (PARTITION BY store_id ORDER BY year, quarter) = 0 THEN NULL
        ELSE ROUND(
            ((revenue - LAG(revenue, 1) OVER (PARTITION BY store_id ORDER BY year, quarter)) 
             / LAG(revenue, 1) OVER (PARTITION BY store_id ORDER BY year, quarter)) * 100, 
            2
        )
    END AS qoq_growth_percent
FROM QuarterlyStoreRevenue
ORDER BY store_name, year, quarter;


-- ----------------------------------------------------------------------------
-- Q13: Total revenue grouped by store, supplier, and product (decision #10).
-- ----------------------------------------------------------------------------
SELECT 
    s.store_name,
    sup.supplier_name,
    p.product_name,
    SUM(f.revenue) AS total_revenue
FROM FACT_SALES f
JOIN DIM_STORE s ON f.store_key = s.store_key
JOIN DIM_SUPPLIER sup ON f.supplier_key = sup.supplier_key
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
GROUP BY s.store_name, sup.supplier_name, p.product_name
ORDER BY s.store_name, sup.supplier_name, total_revenue DESC;


-- ----------------------------------------------------------------------------
-- Q14: Seasonal revenue distribution for each product.
-- ----------------------------------------------------------------------------
SELECT 
    p.product_id,
    p.product_name,
    t.season,
    SUM(f.revenue) AS seasonal_revenue
FROM FACT_SALES f
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
JOIN DIM_TIME t ON f.time_key = t.time_key
GROUP BY p.product_id, p.product_name, t.season
ORDER BY p.product_id, t.season;


-- ----------------------------------------------------------------------------
-- Q15: Month-to-month revenue volatility (%) for each store and supplier combination.
-- ----------------------------------------------------------------------------
WITH MonthlyStoreSupplier AS (
    SELECT 
        s.store_name,
        sup.supplier_name,
        t.year,
        t.month,
        SUM(f.revenue) AS revenue
    FROM FACT_SALES f
    JOIN DIM_STORE s ON f.store_key = s.store_key
    JOIN DIM_SUPPLIER sup ON f.supplier_key = sup.supplier_key
    JOIN DIM_TIME t ON f.time_key = t.time_key
    GROUP BY s.store_name, sup.supplier_name, t.year, t.month
)
SELECT 
    store_name,
    supplier_name,
    year,
    month,
    revenue AS current_month_revenue,
    LAG(revenue, 1) OVER (
        PARTITION BY store_name, supplier_name 
        ORDER BY year, month
    ) AS prev_month_revenue,
    CASE 
        WHEN LAG(revenue, 1) OVER (PARTITION BY store_name, supplier_name ORDER BY year, month) IS NULL THEN NULL
        WHEN LAG(revenue, 1) OVER (PARTITION BY store_name, supplier_name ORDER BY year, month) = 0 THEN NULL
        ELSE ROUND(
            ((revenue - LAG(revenue, 1) OVER (PARTITION BY store_name, supplier_name ORDER BY year, month)) 
             / LAG(revenue, 1) OVER (PARTITION BY store_name, supplier_name ORDER BY year, month)) * 100, 
            2
        )
    END AS monthly_volatility_percent
FROM MonthlyStoreSupplier
ORDER BY store_name, supplier_name, year, month;


-- ----------------------------------------------------------------------------
-- Q16: Top 5 most frequently co-purchased product pairs across all shopping carts (transactions).
-- ----------------------------------------------------------------------------
SELECT 
    p1.product_id AS product_id_1,
    p1.product_name AS product_name_1,
    p2.product_id AS product_id_2,
    p2.product_name AS product_name_2,
    COUNT(DISTINCT f1.transaction_id) AS co_purchase_count
FROM FACT_SALES f1
JOIN FACT_SALES f2 
    ON f1.transaction_id = f2.transaction_id 
    AND f1.product_key < f2.product_key
JOIN DIM_PRODUCT p1 ON f1.product_key = p1.product_key
JOIN DIM_PRODUCT p2 ON f2.product_key = p2.product_key
GROUP BY p1.product_id, p1.product_name, p2.product_id, p2.product_name
ORDER BY co_purchase_count DESC
LIMIT 5;


-- ----------------------------------------------------------------------------
-- Q17: Multi-level sales aggregation using WITH ROLLUP
--      (product, supplier subtotal, store subtotal, grand total).
-- ----------------------------------------------------------------------------
SELECT 
    COALESCE(s.store_name, '--- ALL STORES (GRAND TOTAL) ---') AS store_name,
    COALESCE(sup.supplier_name, '--- ALL SUPPLIERS (STORE TOTAL) ---') AS supplier_name,
    COALESCE(p.product_name, '--- ALL PRODUCTS (SUPPLIER TOTAL) ---') AS product_name,
    SUM(f.revenue) AS total_revenue
FROM FACT_SALES f
JOIN DIM_STORE s ON f.store_key = s.store_key
JOIN DIM_SUPPLIER sup ON f.supplier_key = sup.supplier_key
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
GROUP BY s.store_name, sup.supplier_name, p.product_name WITH ROLLUP;


-- ----------------------------------------------------------------------------
-- Q18: Total quantity and revenue by product, year, and half-year (H1, H2),
--      along with full-year totals.
-- ----------------------------------------------------------------------------
SELECT 
    p.product_id,
    p.product_name,
    t.year,
    COALESCE(t.half_year, 'FULL YEAR') AS period,
    SUM(f.quantity) AS total_quantity,
    SUM(f.revenue) AS total_revenue
FROM FACT_SALES f
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
JOIN DIM_TIME t ON f.time_key = t.time_key
GROUP BY p.product_id, p.product_name, t.year, t.half_year WITH ROLLUP
HAVING p.product_id IS NOT NULL AND t.year IS NOT NULL
ORDER BY p.product_id, t.year, period;


-- ----------------------------------------------------------------------------
-- Q19: High-revenue spike days where daily product revenue exceeds twice its
--      average daily revenue (over sales days only, per decision #11).
-- ----------------------------------------------------------------------------
WITH DailyProductRevenue AS (
    SELECT 
        f.product_key,
        p.product_id,
        p.product_name,
        t.event_date,
        SUM(f.revenue) AS daily_revenue
    FROM FACT_SALES f
    JOIN DIM_PRODUCT p ON f.product_key = p.product_key
    JOIN DIM_TIME t ON f.time_key = t.time_key
    GROUP BY f.product_key, p.product_id, p.product_name, t.event_date
),
ProductAverageDaily AS (
    SELECT 
        product_key,
        AVG(daily_revenue) AS avg_daily_revenue
    FROM DailyProductRevenue
    GROUP BY product_key
)
SELECT 
    dpr.product_id,
    dpr.product_name,
    dpr.event_date,
    dpr.daily_revenue,
    ROUND(pad.avg_daily_revenue, 2) AS avg_daily_revenue,
    ROUND(dpr.daily_revenue / pad.avg_daily_revenue, 2) AS spike_ratio
FROM DailyProductRevenue dpr
JOIN ProductAverageDaily pad ON dpr.product_key = pad.product_key
WHERE dpr.daily_revenue > 2.0 * pad.avg_daily_revenue
ORDER BY dpr.product_id, dpr.event_date;


-- ----------------------------------------------------------------------------
-- Q20: Create ordinary view for quarterly store sales analysis, followed by
--      ordered retrieval (decision #12).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.store_name,
    t.year,
    t.quarter,
    SUM(f.revenue) AS total_revenue,
    SUM(f.quantity) AS total_quantity
FROM FACT_SALES f
JOIN DIM_STORE s ON f.store_key = s.store_key
JOIN DIM_TIME t ON f.time_key = t.time_key
GROUP BY s.store_name, t.year, t.quarter;

-- Retrieval with ORDER BY applied outside the view definition:
SELECT 
    store_name,
    year,
    quarter,
    total_revenue,
    total_quantity
FROM STORE_QUARTERLY_SALES
ORDER BY store_name, year, quarter;

