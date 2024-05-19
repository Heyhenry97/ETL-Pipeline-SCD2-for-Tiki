MERGE INTO dim_product_scd2
        USING (
            SELECT dim_product.tiki_pid AS join_key, dim_product.*
            FROM dim_product
            UNION ALL
            SELECT NULL, dim_product.*
            FROM dim_product
            JOIN dim_product_scd2 ON dim_product.tiki_pid = dim_product_scd2.tiki_pid
            WHERE ((dim_product.name <> dim_product_scd2.name)
                OR (dim_product.origin <> dim_product_scd2.origin)
                OR (dim_product.brand_name <> dim_product_scd2.brand_name))
                AND dim_product_scd2.valid_to IS NULL
                AND dim_product_scd2.is_current = TRUE
        ) sub
        ON sub.join_key = dim_product_scd2.tiki_pid
        WHEN MATCHED AND (sub.name <> dim_product_scd2.name
            OR sub.origin <> dim_product_scd2.origin
            OR sub.brand_name <> dim_product_scd2.brand_name)  
            AND is_current = TRUE
        THEN
            UPDATE SET
                valid_to = sub.ingestion_dt_unix,
                is_current = FALSE
        WHEN NOT MATCHED
        THEN
            INSERT (id, tiki_pid, name, brand_name, origin, ingestion_dt_unix, valid_from, valid_to, is_current)
            VALUES (default, sub.tiki_pid, sub.name, sub.brand_name, sub.origin, sub.ingestion_dt_unix, sub.ingestion_dt_unix, NULL, TRUE);