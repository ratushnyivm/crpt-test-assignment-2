import os

from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '127.0.0.1')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'test_migration')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')
