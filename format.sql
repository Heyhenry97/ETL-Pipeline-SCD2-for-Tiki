INSERT INTO
    dim_product (
        tiki_pid,
        name,
        original_price,
        availability,
        primary_category_name,
        category_l1_name,
        category_l2_name,
        category_l3_name,
        ingestion_dt_unix,
        ingestion_date,
        valid_from,
        valid_to,
        is_active
    )
SELECT
    staging_dim_product.tiki_pid,
    staging_dim_product.name,
    staging_dim_product.original_price,
    staging_dim_product.availability,
    staging_dim_product.primary_category_name,
    staging_dim_product.category_l1_name,
    staging_dim_product.category_l2_name,
    staging_dim_product.category_l3_name,
    staging_dim_product.ingestion_dt_unix,
    staging_dim_product.ingestion_date,
    staging_dim_product.valid_from,
    staging_dim_product.valid_to,
    staging_dim_product.is_active
FROM
    staging_dim_product
    LEFT JOIN dim_product ON dim_product.tiki_pid = staging_dim_product.tiki_pid
WHERE
    dim_product.incremental_product IS NULL;

INSERT INTO
    dim_product (
        tiki_pid,
        name,
        original_price,
        availability,
        primary_category_name,
        category_l1_name,
        category_l2_name,
        category_l3_name,
        ingestion_dt_unix,
        ingestion_date,
        valid_from,
        valid_to,
        is_active
    )
SELECT distinct
    staging_dim_product.tiki_pid,
    staging_dim_product.name,
    staging_dim_product.original_price,
    staging_dim_product.availability,
    staging_dim_product.primary_category_name,
    staging_dim_product.category_l1_name,
    staging_dim_product.category_l2_name,
    staging_dim_product.category_l3_name,
    staging_dim_product.ingestion_dt_unix,
    staging_dim_product.ingestion_date,
    staging_dim_product.valid_from,
    staging_dim_product.valid_to,
    staging_dim_product.is_active
FROM
    staging_dim_product
    LEFT JOIN dim_product ON staging_dim_product.tiki_pid = dim_product.tiki_pid
WHERE
    (
        staging_dim_product.name <> dim_product.name
        OR staging_dim_product.original_price <> dim_product.original_price
        OR staging_dim_product.availability <> dim_product.availability
        OR staging_dim_product.primary_category_name <> dim_product.primary_category_name
        OR staging_dim_product.category_l1_name <> dim_product.category_l1_name
        OR staging_dim_product.category_l2_name <> dim_product.category_l2_name
        OR staging_dim_product.category_l3_name <> dim_product.category_l3_name
    )
    AND dim_product.is_active = true;

UPDATE dim_product
SET
    valid_to = sub.ingestion_dt_unix,
    is_active = false
FROM
    (
        SELECT
            staging_dim_product.incremental_product,
            staging_dim_product.tiki_pid,
            staging_dim_product.original_price,
            staging_dim_product.availability,
            staging_dim_product.primary_category_name,
            staging_dim_product.category_l1_name,
            staging_dim_product.category_l2_name,
            staging_dim_product.category_l3_name,
            staging_dim_product.ingestion_dt_unix,
            staging_dim_product.ingestion_date,
            staging_dim_product.valid_from,
            staging_dim_product.valid_to,
            staging_dim_product.is_active
        FROM
            staging_dim_product
            JOIN dim_product ON staging_dim_product.tiki_pid = dim_product.tiki_pid
        WHERE
            (
                staging_dim_product.name <> dim_product.name
                OR staging_dim_product.original_price <> dim_product.original_price
                OR staging_dim_product.availability <> dim_product.availability
                OR staging_dim_product.primary_category_name <> dim_product.primary_category_name
                OR staging_dim_product.category_l1_name <> dim_product.category_l1_name
                OR staging_dim_product.category_l2_name <> dim_product.category_l2_name
                OR staging_dim_product.category_l3_name <> dim_product.category_l3_name
            )
            AND dim_product.valid_to IS null
    ) AS sub
WHERE
    dim_product.incremental_product = sub.incremental_product;

SELECT
    table_name
FROM
    information_schema.tables
WHERE
    table_name = 'dim_product';

