import pickle
import os
import logging
import hashlib
from pymongo import MongoClient
from datetime import datetime

def save_checkpoint(current_state, progress_point, data):
    filename = f'indexer_checkpoint.pkl'


def save_top_words(index):
    word_counts = {word: sum(url_freq.values()) for word, url_freq in index.items()}
    top_words = dict(sorted(word_counts.items(), key=lambda item: item[1], reverse=True)[:100])
    with open("top_words.pkl", "wb") as f:
        pickle.dump(top_words, f)
    logging.info("Saved top frequent words to 'top_words.pkl'.")

def load_index():
    if os.path.exists("simple_index.pkl"):
        with open("simple_index.pkl", "rb") as f:
            return pickle.load(f)
    else:
        return {}

def save_index(index):
    with open("simple_index.pkl", "wb") as f:
        pickle.dump(index, f)

def hash_url(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def store_indexed_page(url: str, content: str) -> bool:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["search_database"]
    pages_collection = db["indexed_pages"]

    url_hash = hash_url(url)
    existing = pages_collection.find_one({"url_hash": url_hash})

    if existing:
        print(f"[Indexer] Duplicate page skipped: {url}")
        return False

    pages_collection.insert_one({
        "url_hash": url_hash,
        "url": url,
        "content": content,
        "indexed_at": datetime.utcnow()
    })
    print(f"[Indexer] ✅ New page indexed: {url}")
    return True
