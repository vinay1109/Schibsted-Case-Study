CREATE OR REPLACE TABLE user_interest_features AS
WITH user_base AS (
    SELECT
        e.user_id,
        COUNT(DISTINCT e.cart_id) AS cart_count,
        SUM(e.quantity) AS total_items,
        ROUND(SUM(e.line_value), 2) AS total_spend,
        ROUND(AVG(e.unit_price), 2) AS avg_item_price,
        COUNT(DISTINCT e.category) AS distinct_categories,
        MAX(CAST(e.cart_date AS DATE)) AS last_cart_date,
        SUM(CASE WHEN e.is_premium_product = 1 THEN e.line_value ELSE 0 END) AS premium_spend,
        SUM(e.line_value) AS total_line_value
    FROM cart_items_enriched e
    GROUP BY e.user_id
),
category_by_quantity AS (
    SELECT
        user_id,
        category,
        SUM(quantity) AS qty,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY SUM(quantity) DESC, category
        ) AS rn
    FROM cart_items_enriched
    GROUP BY user_id, category
),
category_by_spend AS (
    SELECT
        user_id,
        category,
        SUM(line_value) AS spend,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY SUM(line_value) DESC, category
        ) AS rn
    FROM cart_items_enriched
    GROUP BY user_id, category
)
SELECT
    u.user_id,
    uc.username,
    uc.email,
    uc.city,
    ub.cart_count,
    ub.total_items,
    ub.total_spend,
    CASE
        WHEN ub.cart_count > 0 THEN ROUND(ub.total_spend / ub.cart_count, 2)
        ELSE 0
    END AS avg_cart_value,
    ub.avg_item_price,
    ub.distinct_categories,
    q.category AS top_category_by_quantity,
    s.category AS top_category_by_spend,
    ROUND(
        CASE
            WHEN ub.total_line_value > 0 THEN ub.premium_spend / ub.total_line_value
            ELSE 0
        END,
        4
    ) AS premium_spend_ratio,
    ub.last_cart_date,
    date_diff('day', ub.last_cart_date, current_date) AS days_since_last_cart
FROM user_base ub
JOIN users_stg uc
    ON ub.user_id = uc.user_id
LEFT JOIN category_by_quantity q
    ON ub.user_id = q.user_id AND q.rn = 1
LEFT JOIN category_by_spend s
    ON ub.user_id = s.user_id AND s.rn = 1
JOIN user_base u
    ON ub.user_id = u.user_id
ORDER BY ub.total_spend DESC, ub.user_id;