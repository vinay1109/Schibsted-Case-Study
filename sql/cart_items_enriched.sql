CREATE OR REPLACE TABLE cart_items_enriched AS
WITH price_stats AS (
    SELECT
        median(price) AS median_price
    FROM products_stg
)
SELECT
    c.cart_id,
    c.user_id,
    c.cart_date,
    c.product_id,
    c.quantity,
    p.title,
    p.category,
    p.price AS unit_price,
    p.rating_rate,
    p.rating_count,
    c.quantity * p.price AS line_value,
    CASE
        WHEN p.price > s.median_price THEN 1
        ELSE 0
    END AS is_premium_product
FROM cart_items_stg c
JOIN products_stg p
    ON c.product_id = p.product_id
CROSS JOIN price_stats s;