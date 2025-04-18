import os
import logging
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from pymongo import UpdateOne
from dotenv import load_dotenv

load_dotenv()

# 1) Connection setup
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("Missing MONGO_URI environment variable")

client = MongoClient(MONGO_URI, maxPoolSize=50)
db     = client.get_database(os.getenv("MONGO_DB", "options_db"))
coll   = db.get_collection(os.getenv("MONGO_COLLECTION", "options_data"))

def save_options_data(docs: list) -> None:
    """
    Append‑only: bulk insert today’s snapshot docs into the time‑series collection.
    """
    if not docs:
        logging.warning("save_options_data(): no documents to save.")
        return

    try:
        result = coll.insert_many(docs)
        logging.info("Inserted %d docs into time‑series collection", len(result.inserted_ids))
    except PyMongoError as e:
        logging.error(f"MongoDB insert_many error: {e}")
        raise