from mpi4py import MPI 
import time 
import logging 
import socket
# Import necessary libraries for task queue, database, etc. (e.g., redis, cloud storage SDKs) 
from google.cloud import pubsub_v1
import json


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

TAG_START_CRAWLING = 10
TAG_SEARCH = 11
TAG_NODES_STATUS = 12

project_id = "spheric-arcadia-457314-c8"
topic_id = "crawl-tasks"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def handle_server_requests(comm, status):
    """Handle requests from the server node."""
    if comm.iprobe(source=1, tag=MPI.ANY_TAG, status=status):
        tag = status.Get_tag()
        if tag == TAG_START_CRAWLING:
            # Handle start crawling request
            data = comm.recv(source=1, tag=TAG_START_CRAWLING)
            seed_urls = data.get("seed_urls", [])
            max_depth = data.get("max_depth", 3)
            logging.info(f"Received start crawling request: seed_urls={seed_urls}, max_depth={max_depth}")

            # TODO: Implement actual crawling logic here
            # Simulate crawling and return a list of URLs
            resulted_urls = [f"{url}/page1" for url in seed_urls]  # Example result
            comm.send(resulted_urls, dest=1, tag=TAG_START_CRAWLING)

        elif tag == TAG_SEARCH:
            # Handle search request
            data = comm.recv(source=1, tag=TAG_SEARCH)
            query = data.get("query", "")
            search_type = data.get("search_type", "keyword")
            logging.info(f"Received search request: query={query}, search_type={search_type}")

            # TODO: Implement actual search logic here
            # Simulate search and return a list of URLs
            resulted_urls = [f"http://example.com/{query}/result1", f"http://example.com/{query}/result2"]
            comm.send(resulted_urls, dest=1, tag=TAG_SEARCH)

        elif tag == TAG_NODES_STATUS:
            # Handle nodes status request
            comm.recv(source=1, tag=TAG_NODES_STATUS)  # Receive None
            logging.info("Received nodes status request")

            # TODO: Implement logic to dynamically fetch nodes status from the system
            # Simulate nodes status and return a list of dicts
            nodes_status = [
                {"type": "crawler", "ip_address": "10.10.0.2", "active": True},
                {"type": "indexer", "ip_address": "10.10.0.3", "active": False}
            ]
            comm.send(nodes_status, dest=1, tag=TAG_NODES_STATUS)

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
    urls_to_crawl_queue = seed_urls  # Simple list as initial queue - replace with a distributed queue 
 
    task_count = 0 
    crawler_tasks_assigned = 0 
 
 
    while urls_to_crawl_queue or crawler_tasks_assigned > 0: # Continue as long as there are URLs to crawl or tasks in progress 
        # Handle server requests
        handle_server_requests(comm, status)

        # Check for completed crawler tasks and results from crawler nodes 
        if crawler_tasks_assigned > 0: 
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status): # Non-blocking check for incoming messages 
                message_source = status.Get_source() 
                message_tag = status.Get_tag() 
                message_data = comm.recv(source=message_source, tag=message_tag) 
 
                if message_tag == 1: # Crawler completed task and sent back extracted URLs 
                    crawler_tasks_assigned -= 1 
                    new_urls = message_data['urls'] # Assuming message_data is a list of URLs 
                    #if new_urls: 
                        #urls_to_crawl_queue.extend(new_urls) # Add newly discovered URLs to the queue 
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

        # Assign new crawling tasks if there are URLs in the queue and available crawler nodes 
        while urls_to_crawl_queue and crawler_tasks_assigned < crawler_nodes: # Limit tasks to available crawler nodes for simplicity in this skeleton 
            url_to_crawl = urls_to_crawl_queue.pop(0) # Get URL from queue (FIFO for simplicity) 
            available_crawler_rank = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)] # Simple round-robin assignment 
            task_id = task_count 
            task_count += 1 
            task_metadata = {"urls": [url_to_crawl], "max_depth": 3}  # Example metadata
            # comm.send(task_metadata, dest=available_crawler_rank, tag=0) # Send task with metadata (MPI version, now commented out)
            # --- Pub/Sub version ---
            message_json = json.dumps(task_metadata)
            message_bytes = message_json.encode("utf-8")
            future = publisher.publish(topic_path, message_bytes)
            logging.info(f"Published crawl task to Pub/Sub: {task_metadata}, message ID: {future.result()}")
            crawler_tasks_assigned += 1 
            logging.info(f"Master assigned task {task_id} (crawl {url_to_crawl}) to Pub/Sub topic 'crawl-tasks', Tasks assigned: {crawler_tasks_assigned}") 
            time.sleep(0.1) # Small delay to prevent overwhelming master in this example 
        time.sleep(1) # Master node's main loop sleep - adjust as needed 
    logging.info("Master node finished URL distribution. Waiting for crawlers to complete...") 

    # Send shutdown signal to crawler nodes
    for crawler_rank in active_crawler_nodes:
        comm.send(None, dest=crawler_rank, tag=0)  # Empty task signals shutdown
        logging.info(f"Shutdown signal sent to Crawler {crawler_rank}")

# In a real system, you would have more sophisticated shutdown and result aggregation logic 
print("Master Node Finished.") 
if __name__ == '__main__': 
    master_process()
