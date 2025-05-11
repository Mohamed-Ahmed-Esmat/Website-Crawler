import pickle
import os
import logging
import hashlib
from pymongo import MongoClient
from datetime import datetime
from google.cloud import storage
import subprocess

def upload_file_to_gcs(local_path, gcs_path):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket("bucket-dist")
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        logging.info(f"☁️ Uploaded '{local_path}' to 'gs://bucket-dist/{gcs_path}'")
    except Exception as e:
        logging.error(f"❌ Failed to upload file to GCS: {e}")


def save_checkpoint(current_state, progress_point, data):
    filename = f'indexer_checkpoint.pkl'


def save_top_words(index):
    word_counts = {word: sum(url_freq.values()) for word, url_freq in index.items()}
    top_words = dict(sorted(word_counts.items(), key=lambda item: item[1], reverse=True)[:100])
    with open("top_words.pkl", "wb") as f:
        pickle.dump(top_words, f)
    logging.info("Saved top frequent words to 'top_words.pkl'.")
    upload_file_to_gcs("top_words.pkl", "search_backups/top_words.pkl")


def load_index():
    if os.path.exists("simple_index.pkl"):
        with open("simple_index.pkl", "rb") as f:
            return pickle.load(f)
    else:
        return {}

def save_index(index):
    with open("simple_index.pkl", "wb") as f:
        pickle.dump(index, f)
    upload_file_to_gcs("simple_index.pkl", "search_backups/simple_index.pkl")


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

    try:
        # Store to GCS
        os.makedirs("/tmp/page_backups", exist_ok=True)
        backup_path = f"/tmp/page_backups/{url_hash}.json"
        with open(backup_path, "w") as f:
            import json
            json.dump({
                "url": url,
                "url_hash": url_hash,
                "content": content,
                "indexed_at": datetime.utcnow().isoformat()
            }, f)

        storage_client = storage.Client()
        bucket = storage_client.bucket("bucket-dist")
        blob = bucket.blob(f"page_backups/{url_hash}.json")
        blob.upload_from_filename(backup_path)

        print(f"[GCS] 📤 Page backed up to GCS: page_backups/{url_hash}.json")

    except Exception as e:
        logging.error(f"❌ Failed to back up page to GCS: {e}")

        
    return True



def backup_mongodb_and_upload():
    try:
        logging.info("📤 Dumping MongoDB databases...")
        os.makedirs("/tmp/mongobackup", exist_ok=True)

        subprocess.run(["mongodump", "--db", "search_database", "--out", "/tmp/mongobackup"], check=True)
        subprocess.run(["mongodump", "--db", "indexer", "--out", "/tmp/mongobackup"], check=True)

        logging.info("☁️ Uploading dumps to GCS bucket...")
        storage_client = storage.Client()
        bucket = storage_client.bucket("bucket-dist")

        for root, _, files in os.walk("/tmp/mongobackup"):
            for file in files:
                local_path = os.path.join(root, file)
                remote_path = os.path.relpath(local_path, "/tmp")
                blob = bucket.blob(remote_path)
                blob.upload_from_filename(local_path)
                logging.info(f"✅ Uploaded: {remote_path}")

    except Exception as e:
        logging.error(f"❌ Failed to backup MongoDB and upload to GCS: {e}")
