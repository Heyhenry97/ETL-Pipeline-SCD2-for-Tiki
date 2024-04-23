# Database configuration
DATABASE_CONFIG = {
    'username': 'my_user',
    'password': 'my_password',
    'host': '172.22.0.6',
    'port': '5432',
    'db_name': 'dw_tiki'
}

# Minio configuration
MINIO_CONFIG = {
    'endpoint_url': 'http://172.22.0.5:9000',
    'key': 'minioadmin',
    'secret': 12345678
}

BUCKET_CONFIG = 'tiki'