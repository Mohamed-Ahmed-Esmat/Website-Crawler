from mpi4py import MPI
import time
import logging
import os
import pickle
import re
import socket
from elasticsearch import Elasticsearch
from datetime import datetime
import json
from urllib.parse import urlparse
from elasticsearch_utils import es_manager
from utils import location_extractor
from config import ES_CONFIG, INDEX_SETTINGS
from security import security_manager, require_auth, cors_headers
from ssl_config import setup_cross_cluster_ssl

# Configure logging with IP address
hostname = socket.gethostname()
try:
    ip_address = socket.gethostbyname(hostname)
except:
    ip_address = "unknown-ip"

logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - {ip_address} - Indexer - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"indexer_{ip_address}.log"),
        logging.StreamHandler()
    ]
)

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# Get SSL configuration
ssl_config = setup_cross_cluster_ssl()

# Initialize Elasticsearch client with SSL
es = Elasticsearch(
    [ES_CONFIG['hosts'][0]],
    **ssl_config,
    basic_auth=ES_CONFIG.get('basic_auth'),
    verify_certs=ES_CONFIG.get('verify_certs', True),
    ca_certs=ES_CONFIG.get('ca_certs'),
    client_cert=ES_CONFIG.get('client_cert'),
    client_key=ES_CONFIG.get('client_key')
)

def setup_elasticsearch():
    """Create and configure the Elasticsearch index"""
    index_name = INDEX_SETTINGS['name']
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={
            "settings": INDEX_SETTINGS['settings'],
            "mappings": INDEX_SETTINGS['mappings']
        })
        logging.info(f"Created Elasticsearch index: {index_name}")

# Call setup on startup
setup_elasticsearch()

# --- Checkpointing System Functions ---
def save_checkpoint(current_state, progress_point, data):
    with open('indexer_checkpoint.pkl', 'wb') as f:
        pickle.dump({"state_name": current_state, "progress_point": progress_point, "data": data}, f)

def load_checkpoint():
    if os.path.exists('indexer_checkpoint.pkl'):
        with open('indexer_checkpoint.pkl', 'rb') as f:
            return pickle.load(f)
    return None

# --- Index Functions ---
def load_index():
    if os.path.exists("simple_index.pkl"):
        with open("simple_index.pkl", "rb") as f:
            return pickle.load(f)
    else:
        return {}

def save_index(index):
    with open("simple_index.pkl", "wb") as f:
        pickle.dump(index, f)

# --- State Functions ---
last_heartbeat = time.time()

def idle_state(comm):
    global last_heartbeat
    logging.info("State: IDLE - Waiting for new task...")

    current_time = time.time()
    if current_time - last_heartbeat >= 10:
        logging.info("[IDLE] Heartbeat: Indexer is alive and waiting for tasks.")
        last_heartbeat = current_time

    if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
        page_data = comm.recv(source=MPI.ANY_SOURCE, tag=2)
        if not page_data:
            logging.info("Shutdown signal received. Exiting.")
            return "EXIT", None
        save_checkpoint("Receiving_Data", "received_message", page_data)
        return "Receiving_Data", page_data
    else:
        time.sleep(0.5)
        return "IDLE", None

def receiving_data_state(page_data, progress_point=None):
    logging.info("State: RECEIVING_DATA - Validating input...")
    try:
        # First check if page_data is a valid dictionary
        if not isinstance(page_data, dict):
            logging.warning("Received data is not a dictionary.")
            return "IDLE", None

        # Extract url and content from page_data
        url = page_data.get("url")
        content = page_data.get("content")

        # Now check if content is valid - MOVED THIS AFTER content IS DEFINED
        if progress_point is None or progress_point == "received_message":
            if len(content.split()) < 10:
                logging.warning(f"Received content is too small (only {len(content.split())} words). Skipping indexing.")
                return "IDLE", None

        # Check if url is valid
        if (progress_point is None or progress_point == "received_message") and (not url or not isinstance(url, str)):
            logging.warning("Invalid or missing 'url'.")
            return "IDLE", None

        save_checkpoint("Receiving_Data", "validated_url", page_data)

        # Check if content is valid
        if (progress_point is None or progress_point == "validated_url") and (not content or not isinstance(content, str)):
            logging.warning("Invalid or missing 'content'.")
            return "IDLE", None

        save_checkpoint("Receiving_Data", "validated_content", page_data)
        logging.info(f"Input validated successfully for URL: {url}")
        return "Parsing", {"url": url, "content": content}

    except Exception as e:
        logging.error(f"Error during receiving data validation: {e}")
        return "Recovery", {"original_state": "Receiving_Data", "page_data": page_data}

def parsing_state(data, progress_point=None):
    logging.info("State: PARSING - Starting parsing process...")
    try:
        url = data.get("url")
        content = data.get("content")

        if progress_point is None or progress_point == "validated_content":
            if not content or not isinstance(content, str):
                logging.warning("Invalid content format during parsing.")
                return "IDLE", None
            save_checkpoint("Parsing", "loaded_content", data)
            logging.info("Content loaded successfully.")

        if progress_point is None or progress_point == "loaded_content":
            content = re.sub(r'<[^>]+>', '', content)
            tokens = content.split()
            data["tokens"] = tokens
            save_checkpoint("Parsing", "tokenized", data)
            logging.info(f"Tokenization complete. {len(tokens)} tokens found.")

        if progress_point is None or progress_point == "tokenized":
            clean_words = [word.lower() for word in data["tokens"] if word.isalpha()]
            data["words"] = clean_words
            save_checkpoint("Parsing", "filtered_words", data)
            logging.info(f"Filtering complete. {len(clean_words)} clean words kept.")

        parsed_data = {"url": url, "words": data["words"]}
        return "Indexing", parsed_data

    except Exception as e:
        logging.error(f"Error during parsing: {e}")
        return "Recovery", {"original_state": "Parsing", "data": data}

def save_top_words(index):
    word_counts = {word: sum(url_freq.values()) for word, url_freq in index.items()}
    top_words = dict(sorted(word_counts.items(), key=lambda item: item[1], reverse=True)[:100])
    with open("top_words.pkl", "wb") as f:
        pickle.dump(top_words, f)
    logging.info("Saved top frequent words to 'top_words.pkl'.")

def indexing_state(data, progress_point=None):
    logging.info("State: INDEXING - Starting Elasticsearch indexing process...")
    try:
        url = data.get("url")
        words = data.get("words")
        content = " ".join(words) if words else ""
        
        if not url or not content:
            logging.warning("Missing URL or content for indexing.")
            return "IDLE", None

        # Try to extract location from content or URL
        location = location_extractor.extract_location(content)
        if not location:
            location = location_extractor.extract_from_url(url)

        # Prepare document for Elasticsearch
        doc = {
            "url": url,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            "crawler_rank": rank,
            "domain": urlparse(url).netloc,
            "word_count": len(words),
            "indexed_by": f"indexer_{rank}",
            "suggest": {
                "input": words[:100]  # Use first 100 words for suggestions
            }
        }

        # Add location if found
        if location:
            doc["location"] = location
            logging.info(f"Added location data for URL: {url}")

        # Use asyncio to run the async index_document method
        import asyncio
        success = asyncio.run(es_manager.index_document(doc))
        
        if success:
            logging.info(f"Successfully indexed document for URL: {url}")
            return "Ready_For_Querying", None
        else:
            logging.error(f"Failed to index document for URL: {url}")
            return "Recovery", {"original_state": "Indexing", "data": data}

    except Exception as e:
        logging.error(f"Error during indexing: {e}")
        return "Recovery", {"original_state": "Indexing", "data": data}

def rate_limit_decorator(func):
    """Decorator to implement rate limiting for queries"""
    query_counts = {}
    last_cleanup = time.time()
    
    def cleanup_old_queries():
        nonlocal last_cleanup
        current_time = time.time()
        if current_time - last_cleanup > 3600:  # Cleanup every hour
            for ip in list(query_counts.keys()):
                query_counts[ip] = [t for t in query_counts[ip] if current_time - t < 3600]
                if not query_counts[ip]:
                    del query_counts[ip]
            last_cleanup = current_time
    
    def wrapper(*args, **kwargs):
        ip_address = socket.gethostbyname(socket.gethostname())
        current_time = time.time()
        
        cleanup_old_queries()
        
        # Initialize query count for this IP if not exists
        if ip_address not in query_counts:
            query_counts[ip_address] = []
            
        # Remove queries older than 1 hour
        query_counts[ip_address] = [t for t in query_counts[ip_address] if current_time - t < 3600]
        
        # Check rate limit (1000 queries per hour)
        if len(query_counts[ip_address]) >= 1000:
            print("\nRate limit exceeded. Please wait before making more queries.")
            return "IDLE", None
            
        query_counts[ip_address].append(current_time)
        return func(*args, **kwargs)
        
    return wrapper

class QueryCache:
    def __init__(self, max_size=1000, ttl=3600):
        self.cache = {}
        self.max_size = max_size
        self.ttl = ttl
        self.access_times = {}

    def get(self, key):
        if key in self.cache:
            current_time = time.time()
            if current_time - self.access_times[key] > self.ttl:
                del self.cache[key]
                del self.access_times[key]
                return None
            self.access_times[key] = current_time
            return self.cache[key]
        return None

    def put(self, key, value):
        if len(self.cache) >= self.max_size:
            # Remove oldest item
            oldest_key = min(self.access_times.items(), key=lambda x: x[1])[0]
            del self.cache[oldest_key]
            del self.access_times[oldest_key]
        
        self.cache[key] = value
        self.access_times[key] = time.time()

# Create cache instance
query_cache = QueryCache()

def show_help():
    """Display detailed help information about available commands"""
    help_text = """
Available Commands:
-----------------
1. keyword <term>
   Perform a basic keyword search across all documents
   Example: keyword python programming

2. phrase "<text>"
   Search for an exact phrase match
   Example: phrase "artificial intelligence"

3. fuzzy <term>
   Perform a fuzzy search that tolerates typos
   Example: fuzzy progamming

4. wildcard <pattern>
   Search using wildcard patterns (* and ?)
   Example: wildcard prog*

5. stats
   Show detailed index statistics including:
   - Total documents and size
   - Top domains
   - Word count statistics
   - Location data coverage

6. suggest <prefix>
   Get autocomplete suggestions as you type
   Example: suggest prog

7. advanced
   Enter advanced search mode with filters:
   - Domain filtering
   - Date range
   - Word count range
   - Geographic location radius search

8. help
   Show this help message

9. exit
   Exit query mode

Tips:
-----
- Use quotes for phrases with spaces
- Combine advanced search filters for precise results
- Use fuzzy search when unsure of exact spelling
- Use wildcards for flexible pattern matching
"""
    print(help_text)

@rate_limit_decorator
def ready_for_querying_state(comm):
    logging.info("State: READY_FOR_QUERYING - Accepting Elasticsearch queries...")
    
    print("\n[Indexer] Ready for Queries! Type 'help' for detailed information about commands.")

    while True:
        if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
            page_data = comm.recv(source=MPI.ANY_SOURCE, tag=2)
            if page_data:
                return "Receiving_Data", page_data

        query_input = input("Enter command: ").strip()
        
        if query_input == "exit":
            break
            
        if query_input == "help":
            show_help()
            continue
            
        if query_input == "advanced":
            print("\nAdvanced Search Mode")
            print("Enter search parameters (press Enter to skip):")
            query = input("Search query: ").strip()
            domain = input("Filter by domain (e.g., example.com): ").strip()
            date_from = input("From date (YYYY-MM-DD): ").strip()
            date_to = input("To date (YYYY-MM-DD): ").strip()
            min_words = input("Minimum word count: ").strip()
            max_words = input("Maximum word count: ").strip()
            location = input("Location (lat,lon): ").strip()
            distance = input("Distance from location (e.g., 10km): ").strip()
            
            # Create cache key for advanced search
            cache_key = f"adv_{query}_{domain}_{date_from}_{date_to}_{min_words}_{max_words}_{location}_{distance}"
            cached_result = query_cache.get(cache_key)
            if cached_result:
                results = cached_result
                print("(Cached result)")
            else:
                kwargs = {}
                if domain:
                    kwargs['domain'] = domain
                if date_from:
                    kwargs['from_date'] = date_from
                if date_to:
                    kwargs['to_date'] = date_to
                if min_words or max_words:
                    kwargs['word_count_range'] = {
                        'gte': int(min_words) if min_words else None,
                        'lte': int(max_words) if max_words else None
                    }
                if location and distance:
                    try:
                        lat, lon = map(float, location.split(','))
                        kwargs['location'] = {'lat': lat, 'lon': lon}
                        kwargs['distance'] = distance
                    except ValueError:
                        print("Invalid location format. Should be latitude,longitude")
                        continue
                
                results = es_manager.search(query, search_type="keyword", **kwargs)
                query_cache.put(cache_key, results)
            
        elif query_input.startswith(("keyword ", "phrase ", "fuzzy ", "wildcard ", "suggest ")):
            try:
                cache_key = query_input
                cached_result = query_cache.get(cache_key)
                
                if cached_result:
                    if query_input.startswith("suggest "):
                        suggestions = cached_result
                        print("\nSuggestions (Cached):")
                        for suggestion in suggestions:
                            print(f"- {suggestion}")
                        continue
                    results = cached_result
                    print("(Cached result)")
                else:
                    if query_input.startswith("keyword "):
                        term = query_input[8:].strip()
                        results = es_manager.search(term, search_type="keyword")
                        
                    elif query_input.startswith("phrase "):
                        phrase = query_input[7:].strip('"')
                        results = es_manager.search(phrase, search_type="phrase")
                        
                    elif query_input.startswith("fuzzy "):
                        term = query_input[6:].strip()
                        results = es_manager.search(term, search_type="keyword", fuzziness="AUTO")
                        
                    elif query_input.startswith("wildcard "):
                        pattern = query_input[9:].strip()
                        results = es_manager.search(pattern, search_type="regex")
                        
                    elif query_input.startswith("suggest "):
                        prefix = query_input[8:].strip()
                        suggestions = es_manager.suggest(prefix)
                        query_cache.put(cache_key, suggestions)
                        if suggestions:
                            print("\nSuggestions:")
                            for suggestion in suggestions:
                                print(f"- {suggestion}")
                        else:
                            print("No suggestions found")
                        continue
                        
                    query_cache.put(cache_key, results)
                    
            except Exception as e:
                print(f"Error executing query: {e}")
                continue
                
        elif query_input == "stats":
            try:
                cache_key = "stats"
                cached_stats = query_cache.get(cache_key)
                
                if cached_stats:
                    response = cached_stats
                    print("(Cached statistics)")
                else:
                    response = es_manager.get_stats()
                    query_cache.put(cache_key, response)
                
                print("\nIndex Statistics:")
                print(f"Total Documents: {response['total_docs']}")
                print(f"Total Size: {response['size_mb']:.2f} MB")
                
                if 'domains' in response:
                    print("\nTop 10 Domains:")
                    for domain in response['domains']:
                        print(f"- {domain['key']}: {domain['doc_count']} pages")
                        
                if 'word_count' in response:
                    print(f"\nAverage Words per Page: {response['word_count']['avg']:.0f}")
                    print(f"Min Words: {response['word_count']['min']}")
                    print(f"Max Words: {response['word_count']['max']}")
                    
                if 'locations' in response:
                    print(f"\nPages with Location Data: {response['locations']}")
                continue
                
            except Exception as e:
                print(f"Error getting stats: {e}")
                continue

        else:
            print("Invalid command. Type 'help' for available commands.")
            continue

        # Display results for search queries
        if 'results' in results:
            hits = results['results']
            total = results['total']
            
            print(f"\nFound {total} results:")
            for hit in hits:
                print(f"\n🔹 {hit['url']} (score: {hit['score']:.2f})")
                
                if 'highlights' in hit:
                    print("  Context:")
                    for highlight in hit['highlights']:
                        # Replace markdown-style bold with terminal formatting
                        highlight = highlight.replace('**', '\033[1m').replace('**', '\033[0m')
                        print(f"   {highlight}")
                
                print(f"  Words: {hit['word_count']}")
                print(f"  Domain: {hit['domain']}")
                print(f"  Indexed: {hit['timestamp']}")
                
                if 'location' in hit:
                    print(f"  Location: {hit['location']}")
            
            # Show facets if available
            if 'facets' in results:
                if 'domains' in results['facets']:
                    print("\nTop Domains in Results:")
                    for domain in results['facets']['domains'][:5]:
                        print(f"  {domain['key']}: {domain['doc_count']} pages")
                        
                if 'word_count_stats' in results['facets']:
                    stats = results['facets']['word_count_stats']
                    print(f"\nWord Count Stats in Results:")
                    print(f"  Average: {stats['avg']:.0f}")
                    print(f"  Min: {stats['min']}")
                    print(f"  Max: {stats['max']}")
        else:
            print("No results found")

    return "IDLE", None

# --- Mini Function for Fuzzy Suggest ---
def simple_edit_distance(a, b):
    """Very basic edit distance calculator."""
    if len(a) > len(b):
        a, b = b, a
    if len(b) - len(a) > 2:
        return 999
    return sum(1 for x, y in zip(a, b) if x != y) + abs(len(a) - len(b))


# --- Main Control Function ---
def indexer_node():
    # Get the size for logging
    size = comm.Get_size()
    logging.info(f"Indexer node started with rank {rank} of {size}")
    
    # Clear old checkpoints at startup to avoid processing stale data
    if os.path.exists('indexer_checkpoint.pkl'):
        os.remove('indexer_checkpoint.pkl')
        logging.info("Removed old checkpoint file to start fresh")

    checkpoint = load_checkpoint()
    if checkpoint:
        state = checkpoint["state_name"]
        data = checkpoint["data"]
        progress_point = checkpoint.get("progress_point")
        logging.info(f"Resuming from checkpoint: State={state}, Progress={progress_point}")
    else:
        state = "IDLE"
        data = None
        progress_point = None

    while True:
        if state == "IDLE":
            state, data = idle_state(comm)
            progress_point = None

        elif state == "Receiving_Data":
            state, data = receiving_data_state(data, progress_point)
            progress_point = None

        elif state == "Parsing":
            state, data = parsing_state(data, progress_point)
            progress_point = None

        elif state == "Indexing":
            state, data = indexing_state(data, progress_point)
            progress_point = None

        elif state == "Ready_For_Querying":
            state, data = ready_for_querying_state(comm)
            progress_point = None

        elif state == "EXIT":
            break


if __name__ == "__main__":
    indexer_node()