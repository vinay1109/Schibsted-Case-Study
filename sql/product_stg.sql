CREATE OR REPLACE TABLE products_stg AS
        SELECT
            CAST(product_id AS INTEGER) AS product_id,
            CAST(title AS VARCHAR) AS title,
            CAST(price AS DOUBLE) AS price,
            CAST(category AS VARCHAR) AS category,
            CAST(rating_rate AS DOUBLE) AS rating_rate,
            CAST(rating_count AS INTEGER) AS rating_count
        FROM products_df;