CREATE OR REPLACE TABLE category_popularity_summary AS
SELECT
    category,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT cart_id) AS cart_count,
    SUM(quantity) AS total_items_sold,
    ROUND(SUM(line_value), 2) AS total_revenue,
    ROUND(AVG(unit_price), 2) AS avg_product_price,
    ROUND(AVG(quantity), 2) AS avg_quantity_per_line
FROM cart_items_enriched
GROUP BY category
ORDER BY total_revenue DESC, total_items_sold DESC;