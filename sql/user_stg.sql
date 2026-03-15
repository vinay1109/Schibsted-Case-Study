CREATE OR REPLACE TABLE users_stg AS
        SELECT
            CAST(user_id AS INTEGER) AS user_id,
            CAST(email AS VARCHAR) AS email,
            CAST(username AS VARCHAR) AS username,
            CAST(firstname AS VARCHAR) AS firstname,
            CAST(lastname AS VARCHAR) AS lastname,
            CAST(city AS VARCHAR) AS city,
            CAST(zipcode AS VARCHAR) AS zipcode,
            TRY_CAST(lat AS DOUBLE) AS lat,
            TRY_CAST(lon AS DOUBLE) AS lon
        FROM users_df;