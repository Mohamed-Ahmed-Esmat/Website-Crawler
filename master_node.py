from mpi4py import MPI 
import time 
import logging 
import socket
from flask import Flask, request, jsonify
from threading import Thread
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import os
from functools import wraps
import jwt
from urllib.parse import urlparse
from security import require_auth, cors_headers
from elasticsearch_utils import es_manager
from ssl_config import generate_self_signed_cert

# Configure logging 
hostname = socket.gethostname()
try:
    ip_address = socket.gethostbyname(hostname)
except:
    ip_address = "unknown-ip"
    
logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - {ip_address} - Master - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"master_{ip_address}.log"),
        logging.StreamHandler()
    ]
)

# Elasticsearch configuration
ES_HOST = "http://localhost:9200"
ES_INDEX = "webcrawler"
es = Elasticsearch([ES_HOST])

# JWT Configuration
JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key')  # In production, use proper secret management

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({"error": "No token provided"}), 401
        try:
            token = token.split(" ")[1]  # Remove 'Bearer ' prefix
            jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        except:
            return jsonify({"error": "Invalid token"}), 401
        return f(*args, **kwargs)
    return decorated

# Initialize Flask app
app = Flask(__name__)

# Shared data structures
urls_to_crawl_queue = []  # Queue for URLs to crawl
crawler_status = {"tasks_assigned": 0, "urls_in_queue": 0}  # Status tracking
crawl_results = []  # Store crawl results (for simplicity)

@app.route("/submit", methods=["POST"])
def submit_task():
    """Endpoint to submit a crawling task."""
    data = request.get_json()
    urls = data.get("urls", [])
    max_depth = data.get("max_depth", 3)

    if not urls or not isinstance(urls, list):
        return jsonify({"error": "Invalid 'urls' field. Must be a list of URLs."}), 400

    # Add URLs to the queue
    for url in urls:
        urls_to_crawl_queue.append({"url": url, "max_depth": max_depth})

    crawler_status["urls_in_queue"] = len(urls_to_crawl_queue)
    return jsonify({"message": "Task submitted successfully.", "queue_size": len(urls_to_crawl_queue)}), 200

@app.route("/status", methods=["GET"])
def get_status():
    """Endpoint to get the status of the crawler."""
    return jsonify(crawler_status), 200

@app.route("/results", methods=["GET"])
def get_results():
    """Endpoint to fetch crawl results."""
    return jsonify(crawl_results), 200

@app.route("/search", methods=["GET"])
@require_auth
def search():
    """Advanced search endpoint supporting various search types"""
    query = request.args.get("query", "")
    search_type = request.args.get("type", "keyword")
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    domain = request.args.get("domain")
    sort_by = request.args.get("sort", "_score")
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 10))
    
    # Base query
    search_body = {
        "from": (page - 1) * page_size,
        "size": page_size,
        "sort": [
            {sort_by: {"order": "desc"}}
        ],
        "highlight": {
            "fields": {
                "content": {
                    "number_of_fragments": 3,
                    "fragment_size": 150
                }
            }
        }
    }
    
    # Build query based on search type
    if search_type == "keyword":
        search_body["query"] = {
            "multi_match": {
                "query": query,
                "fields": ["content", "url"],
                "fuzziness": "AUTO"
            }
        }
    elif search_type == "phrase":
        search_body["query"] = {
            "match_phrase": {
                "content": query
            }
        }
    elif search_type == "wildcard":
        search_body["query"] = {
            "wildcard": {
                "content": query
            }
        }
    elif search_type == "regex":
        search_body["query"] = {
            "regexp": {
                "content": query
            }
        }
    
    # Add filters if provided
    filters = []
    if from_date or to_date:
        date_filter = {"range": {"timestamp": {}}}
        if from_date:
            date_filter["range"]["timestamp"]["gte"] = from_date
        if to_date:
            date_filter["range"]["timestamp"]["lte"] = to_date
        filters.append(date_filter)
    
    if domain:
        filters.append({"term": {"domain": domain}})
    
    if filters:
        if "query" in search_body:
            search_body["query"] = {
                "bool": {
                    "must": search_body["query"],
                    "filter": filters
                }
            }
        else:
            search_body["query"] = {"bool": {"filter": filters}}
    
    # Add aggregations
    search_body["aggs"] = {
        "domains": {
            "terms": {
                "field": "domain",
                "size": 10
            }
        },
        "word_count_stats": {
            "stats": {
                "field": "word_count"
            }
        },
        "timeline": {
            "date_histogram": {
                "field": "timestamp",
                "calendar_interval": "day"
            }
        }
    }
    
    try:
        results = es.search(index=ES_INDEX, body=search_body)
        
        # Process and format results
        hits = results["hits"]["hits"]
        formatted_results = []
        
        for hit in hits:
            source = hit["_source"]
            formatted_hit = {
                "url": source["url"],
                "score": hit["_score"],
                "word_count": source["word_count"],
                "timestamp": source["timestamp"],
                "domain": source["domain"]
            }
            
            # Add highlights if available
            if "highlight" in hit:
                formatted_hit["highlights"] = hit["highlight"]["content"]
            
            formatted_results.append(formatted_hit)
        
        # Format aggregations
        aggs = results["aggregations"]
        
        response = {
            "total": results["hits"]["total"]["value"],
            "page": page,
            "page_size": page_size,
            "results": formatted_results,
            "facets": {
                "domains": aggs["domains"]["buckets"],
                "word_count_stats": aggs["word_count_stats"],
                "timeline": aggs["timeline"]["buckets"]
            }
        }
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/suggest", methods=["GET"])
@require_auth
def suggest():
    """Autocomplete suggestions endpoint"""
    prefix = request.args.get("prefix", "")
    
    suggest_body = {
        "suggestions": {
            "prefix": prefix,
            "completion": {
                "field": "content.suggest",
                "size": 5,
                "skip_duplicates": True
            }
        }
    }
    
    try:
        suggestions = es.search(index=ES_INDEX, body=suggest_body)
        options = suggestions["suggest"]["suggestions"][0]["options"]
        
        return jsonify({
            "suggestions": [option["text"] for option in options]
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/auth", methods=["POST"])
def authenticate():
    """Generate JWT token for API access"""
    username = request.json.get("username")
    password = request.json.get("password")
    
    # In production, implement proper authentication
    if username == "admin" and password == "secret":
        token = jwt.encode(
            {"user": username, "exp": datetime.utcnow() + timedelta(hours=24)},
            JWT_SECRET,
            algorithm="HS256"
        )
        return jsonify({"token": token})
    
    return jsonify({"error": "Invalid credentials"}), 401

@app.route("/cross-cluster-search", methods=["GET"])
@require_auth
@cors_headers
def cross_cluster_search():
    """Advanced search endpoint that searches across all connected clusters"""
    query = request.args.get("query", "")
    search_type = request.args.get("type", "keyword")
    include_clusters = request.args.get("clusters", "").split(",")
    
    try:
        # Add cluster-specific parameters
        kwargs = {
            "search_type": search_type,
            "size": int(request.args.get("size", "10")),
            "from": int(request.args.get("from", "0")),
            "cross_cluster": True  # Enable cross-cluster search
        }
        
        if include_clusters:
            kwargs["clusters"] = [c for c in include_clusters if c]
            
        # Call the cross-cluster search
        results = es_manager.search_across_clusters(query, **kwargs)
        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cluster-health", methods=["GET"])
@require_auth
@cors_headers
def cluster_health():
    """Get health status of all connected clusters"""
    try:
        # Get local cluster health
        local_health = es_manager.es.cluster.health()
        
        # Get remote clusters health
        remote_health = {}
        for cluster in es_manager.es.cluster.get_remote_info():
            try:
                cluster_name = cluster['cluster_name']
                connected = cluster['connected']
                if connected:
                    remote_health[cluster_name] = {
                        "connected": True,
                        "num_nodes": cluster['num_nodes_connected'],
                        "max_connections": cluster['max_connections_per_cluster'],
                        "initial_connect_timeout": cluster['initial_connect_timeout']
                    }
                else:
                    remote_health[cluster_name] = {
                        "connected": False,
                        "error": "Cluster not connected"
                    }
            except Exception as cluster_e:
                remote_health[cluster['cluster_name']] = {
                    "connected": False,
                    "error": str(cluster_e)
                }
                
        return jsonify({
            "local_cluster": local_health,
            "remote_clusters": remote_health
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def run_flask_app():
    """Run the Flask app in a separate thread with HTTPS support."""
    # Generate SSL certificate
    key_path, cert_path = generate_self_signed_cert()
    
    # Run Flask with HTTPS
    app.run(
        host="0.0.0.0", 
        port=5000, 
        debug=False,
        ssl_context=(cert_path, key_path)
    )

def master_process(): 
    """ 
    Main process for the master node. 
    Handles task distribution, worker management, and coordination. 
    """ 
    comm = MPI.COMM_WORLD 
    rank = comm.Get_rank() 
    size = comm.Get_size() 
    status = MPI.Status() 
 
    logging.info(f"Master node started with rank {rank} of {size}") 
 
    # Initialize task queue, database connections, etc. 
    # ... (Implementation needed) ... 
 
    crawler_nodes = size - 2 # Assuming master and at least one indexer node 
    indexer_nodes = 1 # At least one indexer node 
 
    if crawler_nodes <= 0 or indexer_nodes <= 0: 
        logging.error("Not enough nodes to run crawler and indexer. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)") 
        return 
 
    active_crawler_nodes = list(range(1, 1 + crawler_nodes)) # Ranks for crawler nodes (assuming rank 0 is master) 
    active_indexer_nodes = list(range(1 + crawler_nodes, size)) # Ranks for indexer nodes 
 
    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}") 
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}") 

    seed_urls = ["http://example.com", "http://example.org"] # Example seed URLs - replace with actual seed URLs 
    urls_to_crawl_queue.extend(seed_urls)  # Simple list as initial queue - replace with a distributed queue 
 
    task_count = 0 
    crawler_tasks_assigned = 0 
 
 
    while urls_to_crawl_queue or crawler_tasks_assigned > 0: # Continue as long as there are URLs to crawl or tasks in progress 
        # Check for completed crawler tasks and results from crawler nodes 
        if crawler_tasks_assigned > 0: 
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status): # Non-blocking check for incoming messages 
                message_source = status.Get_source() 
                message_tag = status.Get_tag() 
                message_data = comm.recv(source=message_source, tag=message_tag) 
 
                if message_tag == 1: # Crawler completed task and sent back extracted URLs 
                    crawler_tasks_assigned -= 1 
                    new_urls = message_data['urls'] # Assuming message_data is a list of URLs 
                    if new_urls: 
                        urls_to_crawl_queue.extend(new_urls) # Add newly discovered URLs to the queue 
                    logging.info(f"Master received URLs from Crawler {message_source}, URLs in queue: {len(urls_to_crawl_queue)}, Tasks assigned: {crawler_tasks_assigned}") 
                elif message_tag == 99: # Crawler node reports status/heartbeat 
                    logging.info(f"Crawler {message_source} status: {message_data}") # Example status message 
                elif message_tag == 999: # Crawler node reports error 
                    logging.error(f"Crawler {message_source} reported error: {message_data}") 
                    crawler_tasks_assigned -= 1 # Decrement task count even on error, consider re-assigning task in real implementation 
                elif message_tag == 98:  # Heartbeat
                    logging.info(f"Heartbeat received from Crawler {message_source}: {message_data}")
                elif message_tag == 2:  # Page content from crawler
                    logging.info(f"Page content received from Crawler {message_source}. Forwarding to indexer...")
                    indexer_rank = active_indexer_nodes[0]  # Assuming one indexer node for now
                    comm.send(message_data, dest=indexer_rank, tag=2)  # Forward to indexer
                    crawl_results.append(message_data)  # Store the result

        # Assign new crawling tasks if there are URLs in the queue and available crawler nodes 
        while urls_to_crawl_queue and crawler_tasks_assigned < crawler_nodes: # Limit tasks to available crawler nodes for simplicity in this skeleton 
            url_to_crawl = urls_to_crawl_queue.pop(0) # Get URL from queue (FIFO for simplicity) 
            available_crawler_rank = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)] # Simple round-robin assignment 
            task_id = task_count 
            task_count += 1 
            task_metadata = {"urls": [url_to_crawl], "max_depth": 3}  # Example metadata
            comm.send(task_metadata, dest=available_crawler_rank, tag=0) # Send task with metadata
            crawler_tasks_assigned += 1 
            logging.info(f"Master assigned task {task_id} (crawl {url_to_crawl}) to Crawler {available_crawler_rank}, Tasks assigned: {crawler_tasks_assigned}") 
            time.sleep(0.1) # Small delay to prevent overwhelming master in this example 
        time.sleep(1) # Master node's main loop sleep - adjust as needed 

        # Update crawler status
        crawler_status["tasks_assigned"] = crawler_tasks_assigned
        crawler_status["urls_in_queue"] = len(urls_to_crawl_queue)

    logging.info("Master node finished URL distribution. Waiting for crawlers to complete...") 

    # Send shutdown signal to crawler nodes
    for crawler_rank in active_crawler_nodes:
        comm.send(None, dest=crawler_rank, tag=0)  # Empty task signals shutdown
        logging.info(f"Shutdown signal sent to Crawler {crawler_rank}")

# In a real system, you would have more sophisticated shutdown and result aggregation logic 
print("Master Node Finished.") 
if __name__ == '__main__':
    # Start the Flask server in a separate thread
    flask_thread = Thread(target=run_flask_app)
    flask_thread.daemon = True
    flask_thread.start()

    # Wait for user input before starting the master process
    input("Press Enter to start the master process...")

    # Start the master process
    master_process()
