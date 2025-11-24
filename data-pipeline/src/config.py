import os
import sys

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),  
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'etl_db'),
    'user': os.getenv('DB_USER', 'user'),
    'password': os.getenv('DB_PASSWORD', 'password'),
    'client_encoding': 'utf8'
}

print("=== Database Configuration ===")
print(f"Host: {DB_CONFIG['host']}")
print(f"Database: {DB_CONFIG['database']}")
print(f"User: {DB_CONFIG['user']}")