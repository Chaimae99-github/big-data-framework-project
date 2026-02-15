import psycopg2

DB_CONFIG = {
    "host": "postgres-gold",
    "database": "datamart",
    "user": "gold_user",
    "password": "gold_password",
    "port": 5432
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)
