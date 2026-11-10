import os

DB_CONFIG = {
    "host": os.getenv("ZOMATO_DB_HOST", "localhost"),
    "user": os.getenv("ZOMATO_DB_USER", "root"),
    "password": os.getenv("ZOMATO_DB_PASSWORD"),
    "database": os.getenv("ZOMATO_DB_NAME", "zomato_analytics")
}