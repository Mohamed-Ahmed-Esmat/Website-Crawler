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
from concurrent.futures import ThreadPoolExecutor
import pymongo

# Added: Get IP for heartbeat
hostname_indexer = socket.gethostname()
try:
    ip_address_indexer = socket.gethostbyname(hostname_indexer)
except socket.gaierror:
    ip_address_indexer = "unknown-ip-indexer"

TAG_INDEXER_HEARTBEAT = 97
TAG_INDEXER_SEARCH_QUERY = 20
TAG_INDEXER_SEARCH_RESULTS = 21

class IndexerStates:
    last_heartbeat = time.time()

    @staticmethod
    def _handle_search_query_task(comm, query_text, search_type, master_rank):
        """Task to perform search and send results back to master via MPI."""
        logging.info(f"Thread Task: Starting search for '{query_text}', type '{search_type}' for master rank {master_rank}")
        results = []
        try:
            if query_text:
                results = IndexerStates.perform_search(query_text, search_type)
                logging.info(f"Thread Task: Search for '{query_text}' yielded {len(results)} results.")
            else:
                logging.warning("Thread Task: Empty search query received.")
            
            comm.send(results, dest=master_rank, tag=TAG_INDEXER_SEARCH_RESULTS)
            logging.info(f"Thread Task: Sent {len(results)} search results for '{query_text}' to master rank {master_rank}.")
            if query_text: # Store non-empty queries
                 store_search_query(query_text)
        except MPI.Exception as e:
            logging.error(f"Thread Task: MPI Error sending search results for '{query_text}': {e}")
        except Exception as e:
            logging.error(f"Thread Task: Error during search query handling for '{query_text}': {e}", exc_info=True)
            # Attempt to send empty results if a non-MPI error occurred before sending
            try:
                if not isinstance(e, MPI.Exception): # Avoid double MPI error logging if send failed
                    comm.send([], dest=master_rank, tag=TAG_INDEXER_SEARCH_RESULTS)
                    logging.info(f"Thread Task: Sent empty results to master due to search error for '{query_text}'.")
            except MPI.Exception as mpi_e:
                logging.error(f"Thread Task: MPI Error sending error-case empty results: {mpi_e}")

    @staticmethod
    def idle_state(comm, executor):
        logging.info("State: IDLE - Waiting for new task...")
        status = MPI.Status()
        current_time = time.time()
        if current_time - IndexerStates.last_heartbeat >= 10:
            rank_indexer = comm.Get_rank()
            heartbeat_data = {
                "node_type": "indexer",
                "rank": rank_indexer,
                "ip_address": ip_address_indexer,
                "timestamp": time.time()
            }
            try:
                comm.send(heartbeat_data, dest=0, tag=TAG_INDEXER_HEARTBEAT)
                logging.info(f"[IDLE] Sent Heartbeat to Master: {heartbeat_data}")
            except MPI.Exception as e:
                logging.error(f"[IDLE] Failed to send heartbeat: {e}")
            IndexerStates.last_heartbeat = current_time

        if comm.iprobe(source=MPI.ANY_SOURCE, tag=2, status=status):
            page_data = comm.recv(source=status.Get_source(), tag=2)
            if page_data is None or (isinstance(page_data, dict) and page_data.get("type") == "shutdown_signal"):
                logging.info("Shutdown signal received via MPI tag 2. Exiting.")
                return "EXIT", None
            logging.info(f"State: IDLE - Received page data from rank {status.Get_source()} via MPI tag 2.")
            return "Receiving_Data", page_data

        if comm.iprobe(source=0, tag=TAG_INDEXER_SEARCH_QUERY, status=status):
            search_request = comm.recv(source=0, tag=TAG_INDEXER_SEARCH_QUERY)
            logging.info(f"State: IDLE - Received search query from master (rank 0): {search_request}")
            
            query_text = search_request.get("query", "")
            search_type = search_request.get("search_type", "keyword")
            
            executor.submit(IndexerStates._handle_search_query_task, comm, query_text, search_type, 0)
            logging.info(f"State: IDLE - Offloaded search for '{query_text}' to executor.")
        
        time.sleep(0.1)
        return "IDLE", None
    
    @staticmethod
    def perform_search(query_text, search_type="fuzzy"):
        logging.info(f"Performing {search_type} search for: '{query_text}'")
        results = []
        try:
            from pymongo import MongoClient
            client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
            db = client["search_database"]
            pages_collection = db["indexed_pages"]

            if search_type == "keyword":
                query_terms = query_text.lower().split()
                all_results_set = set()

                for term in query_terms:
                    if len(term) >= 2:
                        query_regex = {"content": {"$regex": re.escape(term), "$options": "i"}}
                        matching_pages = pages_collection.find(query_regex, {"url": 1, "_id": 0})
                        for page in matching_pages:
                            all_results_set.add(page["url"])
                results = list(all_results_set)

            elif search_type == "fuzzy":
                terms = query_text.lower().split()
                intermediate_results_set = set()

                for term in terms:
                    if len(term) >= 2:
                        term_regex = {"content": {"$regex": re.escape(term), "$options": "i"}}
                        for page in pages_collection.find(term_regex, {"url": 1, "_id": 0}):
                            intermediate_results_set.add(page["url"])
                
                all_found_urls = list(intermediate_results_set)

                if all_found_urls:
                    from collections import Counter
                    result_counter = Counter(all_found_urls)
                    results = [url for url, _ in result_counter.most_common()]
                else:
                    logging.info(f"Fuzzy search for '{query_text}' had no direct term matches, trying broader techniques (if implemented).")

            elif search_type == "wildcard":
                regex_pattern = query_text.replace("*", ".*").replace("?", ".")
                wildcard_regex = {"content": {"$regex": regex_pattern, "$options": "i"}}
                matching_pages = pages_collection.find(wildcard_regex, {"url": 1, "_id": 0})
                results = [page["url"] for page in matching_pages]

            total_found = len(results)
            max_results = 500
            limited_results = results[:max_results]

            logging.info(f"Search for '{query_text}' (type: {search_type}) found {total_found} total matches, returning {len(limited_results)} results.")
            return limited_results

        except pymongo.errors.ConnectionFailure as e:
            logging.error(f"Search Error: MongoDB Connection Failure for query '{query_text}': {e}", exc_info=True)
            return []
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Search Error: MongoDB Operation Failure for query '{query_text}': {e}", exc_info=True)
            return []
        except Exception as e:
            logging.error(f"Search Error: Generic error during search for '{query_text}': {e}", exc_info=True)
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

            if not content or len(content) < 20:
                logging.warning(f"Received content for {url} is too small or empty. Skipping indexing.")
                return "IDLE", None

            if not url or not isinstance(url, str) or not (url.startswith('http://') or url.startswith('https://')):
                logging.warning(f"Invalid or missing 'url': {url}.")
                return "IDLE", None
            
            logging.info(f"Input validated successfully for URL: {url}")

            url_hash = hash_url(url)
            client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)
            pages_collection = client["search_database"]["indexed_pages"]
            if pages_collection.find_one({"url_hash": url_hash}):
                logging.info(f"🔁 URL already indexed: {url} → Skipping.")
                return "IDLE", None
            client.close()

            return "Parsing", {"url": url, "content": content}

        except pymongo.errors.ConnectionFailure as e:
            logging.error(f"Receiving_Data Error: MongoDB Connection Failure for URL '{page_data.get('url')}': {e}", exc_info=True)
            return "IDLE", None
        except Exception as e:
            logging.error(f"Error during receiving data validation for URL '{page_data.get('url')}': {e}", exc_info=True)
            return "Recovery", {"original_state": "Receiving_Data", "page_data": page_data, "error": str(e)}

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
            clean_words = [word.lower() for word in tokens if word.isalpha() and len(word) > 1]

            data["words"] = clean_words
            logging.info(f"Filtering complete for {url}. {len(clean_words)} clean words kept.")
            return "Indexing", {"url": url, "words": clean_words}

        except Exception as e:
            logging.error(f"Error during parsing for {data.get('url')}: {e}", exc_info=True)
            return "Recovery", {"original_state": "Parsing", "data": data, "error": str(e)}

    @staticmethod
    def _perform_indexing_and_backup_task(data_for_indexing):
        """Task to perform actual indexing and backup."""
        url = data_for_indexing.get("url")
        words = data_for_indexing.get("words")
        logging.info(f"Thread Task: Starting indexing for URL: {url}")
        try:
            if not url or not words:
                logging.warning(f"Thread Task: Missing URL or words for indexing {url}. Aborting task.")
                return

            token_count = len(words)
            content_str = " ".join(words)
            doc = {
                "id": url,
                "content_stemmed": content_str,
                "token_count": token_count,
                "autocomplete": content_str,
                "title": {"boost": 2.0, "value": words[0] if words else "untitled"},
                "category": "general",
                "author": "crawler",
                "publish_date": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "suggest_text": content_str
            }

            IndexerSearch.index_document(doc)
            IndexerSearch.index_autocomplete_doc(doc)
            
            store_indexed_page(url, content_str)

            backup_mongodb_and_upload()

            logging.info(f"Thread Task: Indexing and backup complete for URL: {url}.")

        except pysolr.SolrError as e:
            logging.error(f"Thread Task: Solr Error during indexing for {url}: {e}", exc_info=True)
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Thread Task: MongoDB Error during store_indexed_page for {url}: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Thread Task: Generic error during indexing/backup for {url}: {e}", exc_info=True)

    @staticmethod
    def indexing_state(data, executor):
        url = data.get("url")
        logging.info(f"State: INDEXING - Offloading indexing task for URL: {url}")
        
        executor.submit(IndexerStates._perform_indexing_and_backup_task, data)
        
        logging.info(f"State: INDEXING - Task for {url} submitted to executor. Returning to IDLE.")
        return "IDLE", None

    @staticmethod
    def ready_for_querying_state(comm, executor):
        logging.info("State: READY_FOR_QUERYING - (Currently, search queries are handled in IDLE state via MPI)")
        
        status = MPI.Status()
        if comm.iprobe(source=MPI.ANY_SOURCE, tag=2, status=status):
            page_data = comm.recv(source=status.Get_source(), tag=2)
            if page_data is not None and not (isinstance(page_data, dict) and page_data.get("type") == "shutdown_signal"):
                logging.info("READY_FOR_QUERYING: New page data received via MPI! Switching to Receiving_Data...")
                return "Receiving_Data", page_data
            elif page_data is None or (isinstance(page_data, dict) and page_data.get("type") == "shutdown_signal"):
                 logging.info("READY_FOR_QUERYING: Shutdown signal received. Exiting.")
                 return "EXIT", None

        time.sleep(0.1)
        return "IDLE", None

    @staticmethod
    def recovery_state(data, progress_point=None):
        logging.info("State: RECOVERY - Attempting to recover...")
        
        original_state = "Unknown"
        error_details = "N/A"
        url_context = "N/A"

        if isinstance(data, dict):
            original_state = data.get("original_state", "Unknown")
            error_details = data.get("error", "N/A")
            page_data_context = data.get("page_data", data.get("data"))
            if isinstance(page_data_context, dict):
                url_context = page_data_context.get("url", "N/A")
        
        logging.warning(f"Recovery triggered from state: {original_state} for URL context: {url_context}. Error: {error_details}")
        
        time.sleep(1)
        return "IDLE", None

