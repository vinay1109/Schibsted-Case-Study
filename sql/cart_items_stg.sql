CREATE OR REPLACE TABLE cart_items_stg AS
        SELECT
            CAST(cart_id AS INTEGER) AS cart_id,
            CAST(user_id AS INTEGER) AS user_id,
            TRY_CAST(cart_date AS TIMESTAMP) AS cart_date,
            CAST(product_id AS INTEGER) AS product_id,
            CAST(quantity AS INTEGER) AS quantity
        FROM cart_items_df;