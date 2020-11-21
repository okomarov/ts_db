import os

from dotenv import load_dotenv


load_dotenv()


class Config:
    POSTGRES_USER = os.environ.get('POSTGRES_USER', 'postgres')
    POSTGRES_PASS = os.environ.get('POSTGRES_PASS', 'postgres')
    POSTGRES_DATABASE = os.environ.get('POSTGRES_DATABASE', 'postgres')
    MONGODB_URL = os.environ.get('MONGODB_URL', 'mongodb://127.0.0.1:27017/performance')
