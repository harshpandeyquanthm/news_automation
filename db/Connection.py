import os
from urllib.parse import quote_plus
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from dotenv import load_dotenv


load_dotenv()

class DatabaseConnection():

    """
    Class for connecting Mongodb
    """
    _client = None
    _instance = None
    _db = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            self._connect()

    def _connect(self):
        try:
            mongo_url = os.getenv("MONGO_URL", "")
            mongo_user = os.getenv("MONGO_USER", "")
            mongo_password = os.getenv("MONGO_PASSWORD", "")
            mongo_host = os.getenv("MONGO_HOST", "")
            db_name = os.getenv("DB_NAME", "News-fetching")

            if mongo_user and mongo_password and mongo_host:
                encoded_user = quote_plus(mongo_user)
                encoded_pass = quote_plus(mongo_password)
                mongo_url = f"mongodb+srv://{encoded_user}:{encoded_pass}@{mongo_host}/?appName=News"

            self._client = MongoClient(
                mongo_url,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000
            )

            self._client.admin.command('ping')
            self._db = self._client[db_name]
            print("***************************************************")
            print(f"Connection Established with {db_name}")
            print("***************************************************")

        except Exception as e:
            print("Failed to connect to the Mongodb")
            self._client = None
            self._db = None
            raise

    @property
    def db(self):
        return self._db

    def get_collection(self, collection_name: str):
        return self._db[collection_name]

    def close(self):
        if self._client:
            self._client.close()
            print("Mongodb connection closed")


def get_db():
    return DatabaseConnection().db

def get_collection(name: str):
    return DatabaseConnection().get_collection(name)

def close_connection():
    DatabaseConnection().close()


class Collections:
    ARTICLES = "News"   # replace with your collection name
    SOURCES = "sources"
    SCRAPE_LOGS = "scrape_logs"
    ERRORS = "errors"


if __name__ == "__main__":
    try:
        db = get_db()
        print(f"Available collections: {db.list_collection_names()}")
        close_connection()
    except Exception as e:
        print(f"Connection test failed: {e}")
