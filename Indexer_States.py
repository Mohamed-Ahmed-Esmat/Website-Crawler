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
TAG_INDEXER_SEARCH_QUERY = 20 # Master to Indexer for search query
TAG_INDEXER_SEARCH_RESULTS = 21 # Indexer to Master for search results

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

        # Check for search query messages from master
        if comm.iprobe(source=MPI.ANY_SOURCE, tag=TAG_INDEXER_SEARCH_QUERY):
            search_request = comm.recv(source=MPI.ANY_SOURCE, tag=TAG_INDEXER_SEARCH_QUERY)
            logging.info(f"Received search query from master: {search_request}")
            
            query_text = search_request.get("query", "")
            search_type = search_request.get("search_type", "keyword")
            
            if query_text:
                # Process search query and return results to master
                results = IndexerStates.perform_search(query_text, search_type)
                source_rank = MPI.Status().Get_source()
                comm.send(results, dest=0, tag=TAG_INDEXER_SEARCH_RESULTS)
                logging.info(f"Sent search results for '{query_text}' to master: {len(results)} URLs found")
                store_search_query(query_text)  # Store the query in history
            else:
                comm.send([], dest=0, tag=TAG_INDEXER_SEARCH_RESULTS)
                logging.warning("Empty search query received from master")
        
        
        time.sleep(0.5)
        return "IDLE", None
    
    @staticmethod
    def perform_search(query_text, search_type="fuzzy"):
        """
        Perform a search based on the query and search type.
        Returns a list of URLs that match the search criteria.
        """
        logging.info(f"Performing {search_type} search for: '{query_text}'")
        try:
            from pymongo import MongoClient
            client = MongoClient("mongodb://localhost:27017/")
            db = client["search_database"]
            pages_collection = db["indexed_pages"]
            
            results = []
            
            if search_type == "keyword":
                # Simple keyword search in MongoDB
                query_regex = {"content": {"$regex": re.escape(query_text), "$options": "i"}}
                matching_pages = pages_collection.find(query_regex, {"url": 1, "_id": 0})
                results = [page["url"] for page in matching_pages]
                
            elif search_type == "fuzzy":
                # Implement a simple fuzzy search by looking for parts of the query
                terms = query_text.split()
                if terms:
                    # For each term, find documents that contain it
                    term_results = []
                    for term in terms:
                        if len(term) > 3:  # Only consider terms with at least 4 chars for fuzzy matching
                            # Match if the content contains at least part of the term
                            part_regex = {"content": {"$regex": re.escape(term[:-1]), "$options": "i"}}
                            matching_pages = pages_collection.find(part_regex, {"url": 1, "_id": 0})
                            term_results.extend([page["url"] for page in matching_pages])
                    
                    # Count occurrences and sort by frequency
                    from collections import Counter
                    results = [url for url, _ in Counter(term_results).most_common()]
            
            # Limit results to prevent overwhelming the system
            results = results[:100] if len(results) > 100 else results
            
            logging.info(f"Search for '{query_text}' returned {len(results)} results")
            return results
            
        except Exception as e:
            logging.error(f"Error during search: {e}")
            return []


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
            if pages_collection.find_one({"url_hash": url_hash}):
                logging.info(f"🔁 URL already indexed: {url} → Skipping to re-publish only.")
                return "IDLE", None

            # Not indexed → proceed
            return "Parsing", {"url": url, "content": content}

        except Exception as e:
            logging.error(f"Error during receiving data validation: {e}")
            return "Recovery", {"original_state": "Receiving_Data", "page_data": page_data}

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
    def ready_for_querying_state(comm):
        logging.info("State: READY_FOR_QUERYING - Accepting queries (smart search).")
        try:
            if not os.path.exists("simple_index.pkl"):
                logging.error("Index file not found. Cannot perform queries.")
                return "IDLE", None

            # Check for new crawler data
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
                page_data = comm.recv(source=MPI.ANY_SOURCE, tag=2)
                if page_data:
                    logging.info("New crawler data received! Switching to Receiving_Data...")
                    return "Receiving_Data", page_data

            # Don't block on input() - just return to IDLE where we'll handle search requests
            logging.info("Ready for search queries - returning to IDLE state to handle master requests")
            return "IDLE", None

        except Exception as e:
            logging.error(f"Error during querying: {e}")
            return "Recovery", {"original_state": "Ready_For_Querying", "data": None}
        
    @staticmethod
    def recovery_state(data, progress_point=None):
        logging.info("State: RECOVERY - Attempting to recover from error...")
        
        if isinstance(data, dict) and "original_state" in data:
            original_state = data.get("original_state")
            logging.info(f"Recovering from error in state: {original_state}")
        
        time.sleep(1)  
        return "IDLE", None

