/* ==========================================
   KPI Business Analysis Queries (PostgreSQL)
   Project: PySpark Sales Pipeline + KPI Mart
   Schema: kpi
========================================== */


/* 1️⃣ Best Month by Revenue */
SELECT *
FROM kpi.monthly_kpi
ORDER BY monthly_revenue DESC
LIMIT 1;


/* 2️⃣ Best Month by Profit */
SELECT *
FROM kpi.monthly_kpi
ORDER BY monthly_profit DESC
LIMIT 1;


/* 3️⃣ Channel Revenue Share (%) */
SELECT 
    sales_channel,
    revenue,
    ROUND(
        ((revenue / SUM(revenue) OVER ()) * 100)::numeric,
        2
    ) AS revenue_share_pct
FROM kpi.channel_kpi
ORDER BY revenue DESC;


/* 4️⃣ Channel Profit Share (%) */
SELECT 
    sales_channel,
    profit,
    ROUND(
        ((profit / SUM(profit) OVER ()) * 100)::numeric,
        2
    ) AS profit_share_pct
FROM kpi.channel_kpi
ORDER BY profit DESC;


/* 5️⃣ Most Profitable Country (from Top 10 Countries table) */
SELECT *
FROM kpi.top_countries
ORDER BY profit DESC
LIMIT 1;


/* 6️⃣ Top 10 Countries by Profit (sorted) */
SELECT *
FROM kpi.top_countries
ORDER BY profit DESC;


/* 7️⃣ Top Item Types by Revenue (Top 10 already) */
SELECT 
    item_type,
    revenue,
    profit,
    avg_profit_margin_pct
FROM kpi.item_kpi
ORDER BY revenue DESC;


/* 8️⃣ Most Profitable Item Type */
SELECT *
FROM kpi.item_kpi
ORDER BY profit DESC
LIMIT 1;


/* 9️⃣ Priority Revenue Share (%) */
SELECT 
    order_priority,
    revenue,
    ROUND(
        ((revenue / SUM(revenue) OVER ()) * 100)::numeric,
        2
    ) AS revenue_share_pct
FROM kpi.priority_kpi
ORDER BY revenue DESC;


/* 🔟 Priority Profit Share (%) */
SELECT 
    order_priority,
    profit,
    ROUND(
        ((profit / SUM(profit) OVER ()) * 100)::numeric,
        2
    ) AS profit_share_pct
FROM kpi.priority_kpi
ORDER BY profit DESC;


/* 1️⃣1️⃣ Overall KPI Snapshot (single row) */
SELECT
  rows,
  total_revenue,
  total_profit,
  avg_profit_margin_pct
FROM kpi.summary;


/* 1️⃣2️⃣ Simple Growth: Compare first vs last month revenue */
WITH ordered AS (
    SELECT 
        order_year_month,
        monthly_revenue,
        ROW_NUMBER() OVER (ORDER BY order_year_month) AS rn_asc,
        ROW_NUMBER() OVER (ORDER BY order_year_month DESC) AS rn_desc
    FROM kpi.monthly_kpi
),
first_last AS (
    SELECT
        MAX(CASE WHEN rn_asc = 1 THEN order_year_month END) AS first_month,
        MAX(CASE WHEN rn_asc = 1 THEN monthly_revenue END) AS first_revenue,
        MAX(CASE WHEN rn_desc = 1 THEN order_year_month END) AS last_month,
        MAX(CASE WHEN rn_desc = 1 THEN monthly_revenue END) AS last_revenue
    FROM ordered
)
SELECT
    first_month,
    first_revenue,
    last_month,
    last_revenue,
    ROUND(
        (((last_revenue - first_revenue) / first_revenue) * 100)::numeric,
        2
    ) AS revenue_growth_pct
FROM first_last;
