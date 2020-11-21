import psycopg2
import pymongo
from arctic import Arctic

from config import Config


def get_postgres_db():
    return psycopg2.connect(
        user=Config.POSTGRES_USER,
        password=Config.POSTGRES_PASS,
        dbname=Config.POSTGRES_DATABASE)


def get_mongo_db():
    mongodb = pymongo.MongoClient(Config.MONGODB_URL)
    return mongodb.get_default_database()


def get_mongo_arctic():
    return Arctic(Config.MONGODB_URL)
