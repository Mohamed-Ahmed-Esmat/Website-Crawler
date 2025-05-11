from mpi4py import MPI
import time
import logging
import os
import pickle
import re
import pysolr
from search_utils import IndexerSearch
from utils import hash_url, store_indexed_page
from history_utils import store_search_query, get_search_history
from utils import backup_mongodb_and_upload
import socket

# Added: Get IP for heartbeat
hostname_indexer = socket.gethostname()
try:
    ip_address_indexer = socket.gethostbyname(hostname_indexer)
except socket.gaierror:
    ip_address_indexer = "unknown-ip-indexer"

TAG_INDEXER_HEARTBEAT = 97 # Added: New tag for indexer heartbeat

class IndexerStates:
    last_heartbeat = time.time()

    @staticmethod
    def idle_state(comm):
        logging.info("State: IDLE - Waiting for new task...")
        current_time = time.time()
        if current_time - IndexerStates.last_heartbeat >= 10:
            # logging.info("[IDLE] Heartbeat: Indexer is alive and waiting for tasks.") # Replaced by new MPI heartbeat to master
            
            rank_indexer = comm.Get_rank() # Get rank within the method
            heartbeat_data = {
                "node_type": "indexer",
                "rank": rank_indexer,
                "ip_address": ip_address_indexer,
                "timestamp": time.time()
            }
            comm.send(heartbeat_data, dest=0, tag=TAG_INDEXER_HEARTBEAT)
            logging.info(f"[IDLE] Sent Heartbeat to Master: {heartbeat_data}")
            IndexerStates.last_heartbeat = current_time

        if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
            page_data = comm.recv(source=MPI.ANY_SOURCE, tag=2)
            if not page_data:
                logging.info("Shutdown signal received. Exiting.")
                return "EXIT", None
            return "Receiving_Data", page_data
        else:
            time.sleep(0.5)
            return "EXIT", None

    @staticmethod
    def receiving_data_state(page_data, progress_point=None):
        logging.info("State: RECEIVING_DATA - Validating input...")
        try:
            if not isinstance(page_data, dict):
                logging.warning("Received data is not a dictionary.")
                return "IDLE", None

            url = page_data.get("url")
            content = page_data.get("content")

            if progress_point is None or progress_point == "received_message":
                if len(content.split()) < 10:
                    logging.warning("Received content is too small. Skipping indexing.")
                    return "IDLE", None

            if not url or not isinstance(url, str):
                logging.warning("Invalid or missing 'url'.")
                return "IDLE", None

            if not content or not isinstance(content, str):
                logging.warning("Invalid or missing 'content'.")
                return "IDLE", None

            logging.info(f"Input validated successfully for URL: {url}")

            # Check if already indexed
            url_hash = hash_url(url)
            from pymongo import MongoClient
            client = MongoClient("mongodb://localhost:27017/")
            pages_collection = client["search_database"]["indexed_pages"]
            #if pages_collection.find_one({"url_hash": url_hash}):
            #    logging.info(f"🔁 URL already indexed: {url} → Skipping to re-publish only.")
            #    return "IDLE", None

            # Not indexed → proceed
            return "Parsing", {"url": url, "content": content}

        except Exception as e:
            logging.error(f"Error during receiving data validation: {e}")
            return "IDLE", {"original_state": "Receiving_Data", "page_data": page_data}

    @staticmethod
    def parsing_state(data, progress_point=None):
        logging.info("State: PARSING - Starting parsing process...")
        try:
            url = data.get("url")
            content = data.get("content")

            if not content or not isinstance(content, str):
                logging.warning("Invalid content format during parsing.")
                return "IDLE", None

            content = re.sub(r'<[^>]+>', '', content)
            tokens = content.split()
            clean_words = [word.lower() for word in tokens if word.isalpha()]

            data["words"] = clean_words
            logging.info(f"Filtering complete. {len(clean_words)} clean words kept.")
            return "Indexing", {"url": url, "words": clean_words}

        except Exception as e:
            logging.error(f"Error during parsing: {e}")
            return "Recovery", {"original_state": "Parsing", "data": data}

    @staticmethod
    def indexing_state(data, progress_point=None):
        logging.info("State: INDEXING - Starting enhanced Solr indexing process...")
        try:
            url = data.get("url")
            words = data.get("words")

            if not url or not words:
                logging.warning("Missing URL or words for indexing.")
                return "IDLE", None

            token_count = len(words)
            content_str = " ".join(words)
            doc = {
                "id": url,
                "content_stemmed": content_str,
                "token_count": token_count,
                "autocomplete": content_str,
                "title": {"boost": 2.0, "value": words[0] if words else "untitled"},
                "category": "news",
                "author": "unknown",
                "publish_date": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "suggest_text": content_str
            }

            IndexerSearch.index_document(doc)
            IndexerSearch.index_autocomplete_doc(doc)
            
            # Save page record
            store_indexed_page(url, content_str)

            #Backup MongoDB
            backup_mongodb_and_upload()

            logging.info(f"✔️ Indexing complete for URL: {url}.")


            return "Ready_For_Querying", None

        except Exception as e:
            logging.error(f"Error during indexing: {e}")
            return "Recovery", {"original_state": "Indexing", "data": data}

    @staticmethod
    def search(query, search_type="keyword"):
        logging.info(f"State: SEARCHING - Executing {search_type} search for: '{query}'")
        try:
            if search_type == "keyword":
                # Exact keyword search
                result = IndexerSearch.search_stemmed_query(query)

            if search_type == "fuzzy":
                # Fuzzy search for typos/variations
                result = IndexerSearch.fuzzy_query_search(query)
            
            return result

        except Exception as e:
            logging.error(f"Error during search: {e}")
            return False