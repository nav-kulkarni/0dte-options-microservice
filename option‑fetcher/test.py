import os
import sys
import traceback
from datetime import datetime
from dotenv import load_dotenv

# Load .env for MONGO_URI, etc.
load_dotenv()

# Import modules under test
try:
    from utils import fetch_options_data
    from db_operations import save_options_data, coll
    module_source = 'utils.py & db_operations.py'
except ImportError:
    print("ERROR: Could not import utils.py or db_operations.py. Make sure they exist in the project root.")
    traceback.print_exc()
    sys.exit(1)


def test_fetch():
    """
    Ensure fetch_options_data returns a list of dicts with required keys.
    """
    print(f"Running fetch_options_data test (from {module_source})...")
    docs = fetch_options_data("SPY")
    if not docs:
        print("fetch_options_data returned no data. Check ticker and connectivity.")
        return
    assert isinstance(docs, list), "Expected a list of dicts"
    doc0 = docs[0]
    required = {"ticker","expiration_date","option_type","strike","open_interest","ts"}
    missing = required - set(doc0.keys())
    if missing:
        print(f"Missing keys in first doc: {missing}")
    else:
        print(f"fetch test passed: {len(docs)} docs, sample keys: {sorted(doc0.keys())}")


def test_db_append_only():
    """
    Ensure save_options_data appends docs to a time-series collection.
    Inserting twice should double the document count.
    """
    print(f"Running db append-only test (from {module_source})...")
    if not os.getenv("MONGO_URI"):
        print("MONGO_URI not set; skipping DB test.")
        return

    now = datetime.now()
    # Two dummy docs for TEST ticker
    docs = [
        {"ticker":"TEST","expiration_date":now,"option_type":"call","strike":1.0,
         "open_interest":10,"ts":now},
        {"ticker":"TEST","expiration_date":now,"option_type":"call","strike":2.0,
         "open_interest":20,"ts":now}
    ]
    # Clean slate
    coll.delete_many({"ticker":"TEST"})

    # First insert
    save_options_data(docs)
    count1 = coll.count_documents({"ticker":"TEST"})
    if count1 != len(docs):
        print(f"ERROR: Expected {len(docs)} docs after first insert, found {count1}")
    else:
        print(f"First insert test passed: {count1} docs.")

    # Second insert should append again
    save_options_data(docs)
    count2 = coll.count_documents({"ticker":"TEST"})
    expected = len(docs) * 2
    if count2 != expected:
        print(f"ERROR: Expected {expected} docs after second insert, found {count2}")
    else:
        print(f"Append test passed: {count2} docs after second insert.")


if __name__ == "__main__":
    test_fetch()
    print()
    test_db_append_only()
