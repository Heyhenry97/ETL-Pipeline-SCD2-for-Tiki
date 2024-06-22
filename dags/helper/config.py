fact_sales = [
    "tiki_epoch",
    "tiki_pid",
    "name",
    "price",
    "discount",
    "discount_rate",
    "rating_average",
    "review_count",
    "quantity_sold_value",
    "ingestion_date",
]

dim_product = [
    "tiki_pid",
    "name",
    "original_price",
    "availability",
    "primary_category_name",
    "category_l1_name",
    "category_l2_name",
    "category_l3_name",
    "ingestion_dt_unix",
    "ingestion_date",
    "valid_from",
    "valid_to",
    "is_active",
]

dim_product_table = """
        CREATE TABLE IF NOT EXISTS dim_product (
            incremental_product SERIAL PRIMARY KEY,
            tiki_pid TEXT,
            name TEXT,
            original_price INT,
            availability INT,
            primary_category_name TEXT,
            category_l1_name TEXT,
            category_l2_name TEXT,
            category_l3_name TEXT,
            ingestion_dt_unix BIGINT,
            ingestion_date TIMESTAMP,
            valid_from BIGINT,
            valid_to BIGINT,
            is_active BOOLEAN DEFAULT true
        );
        """
        
        
staging_dim_product_table = """
        CREATE TABLE IF NOT EXISTS staging_dim_product (
            incremental_product SERIAL PRIMARY KEY,
            tiki_pid TEXT,
            name TEXT,
            original_price INT,
            availability INT,
            primary_category_name TEXT,
            category_l1_name TEXT,
            category_l2_name TEXT,
            category_l3_name TEXT,
            ingestion_dt_unix BIGINT,
            ingestion_date TIMESTAMP,
            valid_from BIGINT,
            valid_to BIGINT,
            is_active BOOLEAN DEFAULT true
        );
        """


fact_sales_table = """
        CREATE TABLE IF NOT EXISTS fact_sales (
            tiki_epoch TEXT PRIMARY KEY,
            tiki_pid INT,
            incremental_product INT,
            name TEXT,
            price INT,
            discount INT,
            discount_rate INT,
            rating_average DECIMAL,
            review_count INT,
            quantity_sold_value INT,
            ingestion_date TIMESTAMP
        );
        """

incremental_product  ="""
       UPDATE fact_sales fs
        SET incremental_product = dp.incremental_product
        FROM dim_product dp
        WHERE fs.tiki_pid = CAST(dp.tiki_pid AS INT)
        AND dp.is_active = true;
        """


insert_change_record_dim_product = """
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
"""


update_change_record_dim_product = """
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
"""

insert_new_record_dim_product = """
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
"""



header = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "x-source": "local",
    "Host": "api.tiki.vn",
}
db_url = "postgresql://my_user:my_password@pg-tiki:5432/dw_tiki"
bucket = "ae-project"
url_template = "https://api.tiki.vn/seller-store/v2/collections/5/products?seller_id=1&limit=40&cursor={cursor}&category=8322&delivery_zone=Vk4wMzkwMDYwMDE%3D"


tiki_pid_list = [
    74021317,
    188940817,
    189643105,
    26114399,
    109017985,
    52789367,
    69764541,
    174444163,
    73207240,
    121635154,
    147920903,
    3954355,
    73787185,
    117254517,
    42230121,
    275243138,
    117238177,
    143217512,
    72882553,
    157432544,
    121635152,
    190861557,
    196515658,
    194959173,
    72459686,
    195969664,
    68585576,
    106863963,
    145288029,
    111285062,
    85763211,
    3304875,
    10095276,
    113530805,
    168283405,
    161008715,
    54614797,
    3639597,
    4780917,
    17336364,
    71345381,
    92859698,
    10005245,
    8885995,
    184466860,
    52788072,
    161008717,
    114937969,
    78598381,
    161310103,
    15267827,
    196932307,
    49747007,
    54266567,
    76388962,
    187338036,
    53720978,
    57325187,
    171556602,
    77774625,
    75307228,
    3014291,
    19792256,
    109505434,
    90622579,
    104769768,
    20917699,
    112110014,
    7833728,
    69978860,
    197558907,
    173748415,
    80628929,
    147999778,
    138107485,
    103640412,
    91947689,
    195969669,
    168293266,
    103379147,
    174215347,
    72308265,
    5067169,
    56454897,
    196923457,
    23416722,
    197344439,
    74341297,
    82912488,
    197344441,
    7718737,
    184070223,
    27582931,
    139303869,
    140416370,
    72001057,
    76013378,
    157255752,
    8835159,
    127844385,
    181699664,
]


tiki_pid_list_staging = [
    74021317, 188940817, 189643105, 26114399, 109017985, 52789367, 69764541, 174444163,
    73207240, 121635154, 147920903, 3954355, 73787185, 117254517, 42230121, 275243138,
    117238177, 143217512, 72882553, 157432544, 121635152, 190861557, 196515658, 194959173,
    72459686, 195969664, 68585576, 106863963, 145288029, 111285062, 85763211, 3304875,
    10095276, 113530805, 168283405, 161008715, 54614797, 3639597, 4780917, 17336364,
    71345381, 92859698, 10005245, 8885995, 184466860, 52788072, 161008717, 114937969,
    78598381, 161310103, 15267827, 196932307, 49747007, 54266567, 76388962, 187338036,
    53720978, 57325187, 171556602, 77774625, 75307228, 3014291, 19792256, 109505434,
    90622579, 104769768, 20917699, 112110014, 7833728, 69978860, 197558907, 173748415,
    80628929, 147999778, 138107485, 103640412, 91947689, 195969669, 168293266, 103379147,
    174215347, 72308265, 5067169, 56454897, 196923457, 23416722, 197344439, 74341297,
    82912488, 7718737, 184070223, 27582931, 139303869, 140416370, 72001057, 76013378,
    157255752, 8835159, 127844385, 181699664, 148760924, 194205608, 59262880, 192115858,
    196851308, 167972598, 107802183, 21367457, 173882437, 94173458, 120219394, 179546672,
    185067342, 173763072, 108684605, 85396163, 163703151, 196409700, 55605170, 76776245,
    52329763, 105483727, 194205620, 124767703, 105663942, 179735872, 195187480, 171549981,
    154245455, 69219919, 158019962, 8886007, 100931416, 71907876, 540039, 168234714,
    13333355, 139567621, 196422809, 191920180, 149089470, 126743459, 194960246, 196851826,
    35191892, 104741661, 54422942, 167210779, 196016781, 152740748
]
