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
TAG_SHUTDOWN_MASTER = 13 # New tag for graceful shutdown
TAG_INDEXER_HEARTBEAT = 97 # Added: For Indexer Heartbeats
TAG_INDEXER_SEARCH_QUERY = 20 # Master to Indexer for search query
TAG_INDEXER_SEARCH_RESULTS = 21 # Indexer to Master for search results

NODE_HEARTBEAT_TIMEOUT = 60 # Seconds. If no heartbeat received within this time, node is considered inactive.
node_info_map = {} # MODULE-LEVEL GLOBAL: Stores details of connected nodes

project_id = "spheric-arcadia-457314-c8"
topic_id = "crawl-tasks"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# --- Helper function to get node statuses ---
def get_updated_node_statuses(): # No parameters needed if using global node_info_map and NODE_HEARTBEAT_TIMEOUT
    """
    Updates the 'active' status in the global node_info_map based on last_seen time
    and generates a list of node statuses for server requests.
    Returns:
        list: A list of dictionaries, e.g., 
              [{"type": "crawler", "ip_address": "10.10.0.2", "active": True}, ...]
    """
    global node_info_map, NODE_HEARTBEAT_TIMEOUT # Explicitly use global
    server_status_list = []
    current_time = time.time()
    ranks_to_delete = [] # If we want to remove very old entries

    for rank_key, details in list(node_info_map.items()): # Iterate over a copy if modifying
        if current_time - details.get("last_seen", 0) > NODE_HEARTBEAT_TIMEOUT:
            node_info_map[rank_key]["active"] = False
            # Optionally, if a node is inactive for too long, consider removing it
            # if current_time - details.get("last_seen", 0) > NODE_HEARTBEAT_TIMEOUT * 10: 
            #     ranks_to_delete.append(rank_key)
        else:
            node_info_map[rank_key]["active"] = True # Ensure it's marked active if within timeout

        server_status_list.append({
            "type": details.get("type", "unknown"),
            "ip_address": details.get("ip", "N/A"),
            "active": node_info_map[rank_key]["active"]
        })
    
    # for rank_key in ranks_to_delete:
    #     del node_info_map[rank_key]
    #     logging.info(f"Master: Removed stale node entry for rank {rank_key} due to prolonged inactivity.")

    return server_status_list
# --- End Helper --- 

def handle_server_requests(comm, status):
    """
    Handle requests from the server node (assumed to be rank 1).
    For TAG_START_CRAWLING, it returns job info to master_process.
    For other tags, it handles them directly and sends a response.
    """
    job_info_to_return = None
    # Check for messages specifically from source 1 (the server node)
    if comm.iprobe(source=1, tag=MPI.ANY_TAG, status=status):
        tag = status.Get_tag()
        source_rank = status.Get_source() # This will be 1

        if tag == TAG_START_CRAWLING:
            # Handle start crawling request
            data = comm.recv(source=1, tag=TAG_START_CRAWLING)
            seed_urls = data.get("seed_urls", [])
            max_depth = data.get("max_depth", 3)
            logging.info(f"Master: Received new crawl job request from server (rank {source_rank}): URLs={seed_urls}, depth={max_depth}")
            job_info_to_return = {
                "type": "crawl_job",
                "seed_urls": seed_urls,
                "max_depth": max_depth,
                "reply_to_rank": source_rank,
                "reply_tag": TAG_START_CRAWLING # Master will use this tag to send the final list
            }
            # IMPORTANT: No comm.send back to server here; master_process will do it when job is done.

        elif tag == TAG_SEARCH:
            # Handle search request directly
            data = comm.recv(source=source_rank, tag=TAG_SEARCH) # source_rank is server (rank 1)
            query_text = data.get("query", "")
            search_type = data.get("search_type", "keyword")
            logging.info(f"Master: Received search request from server (rank {source_rank}): query='{query_text}', type='{search_type}'")

            active_indexer_rank = None
            # Find an active indexer from the global node_info_map
            for node_r, details in node_info_map.items():
                if details.get("type") == "indexer" and details.get("active") is True:
                    active_indexer_rank = node_r
                    break # Found one, use it
            
            resulted_urls = [] # Default to empty list

            if active_indexer_rank is not None:
                logging.info(f"Master: Forwarding search query '{query_text}' to active Indexer Rank {active_indexer_rank}")
                search_payload = {"query": query_text, "search_type": search_type}
                try:
                    comm.send(search_payload, dest=active_indexer_rank, tag=TAG_INDEXER_SEARCH_QUERY)
                    
                    # Assume indexer will reply with a list of URLs
                    # This is a blocking receive. Consider adding a timeout or non-blocking mechanism for robustness in a full system.
                    logging.info(f"Master: Waiting for search results from Indexer Rank {active_indexer_rank}...")
                    resulted_urls = comm.recv(source=active_indexer_rank, tag=TAG_INDEXER_SEARCH_RESULTS)
                    if not isinstance(resulted_urls, list):
                        logging.warning(f"Master: Received malformed search results from Indexer {active_indexer_rank} (expected list, got {type(resulted_urls)}). Defaulting to empty list.")
                        resulted_urls = []
                    else:
                        logging.info(f"Master: Received {len(resulted_urls)} search results from Indexer Rank {active_indexer_rank}.")
                except Exception as e:
                    logging.error(f"Master: Error communicating with Indexer Rank {active_indexer_rank} for search: {e}")
                    resulted_urls = [] # Send empty list on error
            else:
                logging.warning("Master: No active indexer found to handle search query.")
                # resulted_urls is already an empty list

            comm.send(resulted_urls, dest=source_rank, tag=TAG_SEARCH) # Send results (or empty list) back to server
            logging.info(f"Master: Sent {len(resulted_urls)} search results for '{query_text}' back to server (rank {source_rank})")

        elif tag == TAG_NODES_STATUS:
            # Handle nodes status request directly
            comm.recv(source=source_rank, tag=TAG_NODES_STATUS) # Expecting None
            logging.info(f"Master: Received nodes status request from server (rank {source_rank})")
            
            # Now calls the global-aware helper function
            nodes_status = get_updated_node_statuses() 
            comm.send(nodes_status, dest=source_rank, tag=TAG_NODES_STATUS)
            logging.info(f"Master: Sent node status ({len(nodes_status)} entries) back to server (rank {source_rank})")

        elif tag == TAG_SHUTDOWN_MASTER:
            logging.info(f"Master: Received shutdown signal from server (rank {source_rank}).")
            # Potentially receive any data if the protocol expects it, though likely None for a shutdown
            comm.recv(source=source_rank, tag=TAG_SHUTDOWN_MASTER) 
            job_info_to_return = {"type": "shutdown"}

        else:
            # Handle other unexpected tags from server if necessary
            logging.warning(f"Master: Received message with unhandled tag {tag} from server (rank {source_rank})")
            # Optionally receive it to clear the buffer if data is expected
            # data = comm.recv(source=source_rank, tag=tag)

    return job_info_to_return

def master_process():
    """
    Main process for the master node.
    Handles task distribution, worker management, and coordination.
    Now operates in a job-oriented manner, processing one crawl job at a time.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()
    
    global node_info_map

    # node_info_map is now global, master_process will use the global instance.
    logging.info(f"Master node (rank {rank}) started with world size {size}. Using global node_info_map.")

    # Node configuration (assuming server is rank 1 and not part of these calcs for crawler/indexer roles)
    # If server (rank 1) is exclusively for API, then it's not a crawler or indexer.
    # The original calculation:
    # crawler_nodes = size - 2 # Assuming master and at least one indexer node
    # Let's adjust if server is a dedicated rank:
    # Assuming master=0, server=1, then other roles start from rank 2.
    # Number of worker nodes available for crawling/indexing = size - 2 (master, server)
    
    # Sticking to user's current node counting for now, and assuming server is rank 1.
    # User's master_node.py (from previous context) has:
    crawler_nodes_count = size - 2 # Assuming this means (size - master - at_least_one_indexer)
    indexer_nodes_count = 1      # Assuming at least one

    if crawler_nodes_count <= 0: # Simplified check
        logging.error(f"Master: Not enough nodes for crawlers. Need at least 3 nodes in total (master, server, 1 crawler). Found size={size}")
        return
    
    # These rank assignments assume server @ 1 is NOT a crawler/indexer.
    # If server @ 1 IS a crawler, these ranges need adjustment.
    # For Pub/Sub, crawler ranks are less critical for master->crawler task send.
    # But they ARE used for shutdown signals and potentially for indexer communication.
    # The original code had active_crawler_nodes starting from 1.
    # If server is rank 1, and crawlers need distinct MPI ranks for comms (e.g. shutdown)
    # then crawlers should start from rank 2.
    # For now, let's use the original logic for active_crawler_nodes and active_indexer_nodes
    # and assume server at rank 1 is handled distinctly by handle_server_requests.
    active_crawler_node_ranks = list(range(1, 1 + crawler_nodes_count)) # Ranks for crawler nodes (original logic)
    active_indexer_node_ranks = list(range(1 + crawler_nodes_count, size)) # Ranks for indexer nodes (original logic)
    
    logging.info(f"Master: Deduced Active Crawler Node MPI Ranks: {active_crawler_node_ranks} (count: {crawler_nodes_count})")
    logging.info(f"Master: Deduced Active Indexer Node MPI Ranks: {active_indexer_node_ranks} (count: {len(active_indexer_node_ranks)})")
    logging.info("Master: Initialized. Waiting for crawl job requests from server (rank 1)...")

    # Main loop to listen for and process crawl jobs one by one
    while True:
        # Check for any incoming requests from the server (rank 1)
        # This call will return job details if a new crawl job is posted, or None otherwise.
        # It will also handle direct/immediate requests like status or search.
        current_job_details = handle_server_requests(comm, status)

        if current_job_details and current_job_details.get("type") == "shutdown":
            logging.info("Master: Shutdown instruction received. Exiting main processing loop.")
            break # Exit the while True loop to proceed to shutdown logic

        if current_job_details and current_job_details["type"] == "crawl_job":
            job_seed_urls = current_job_details["seed_urls"]
            job_max_depth = current_job_details["max_depth"]
            job_reply_to_rank = current_job_details["reply_to_rank"]
            job_reply_tag = current_job_details["reply_tag"]

            logging.info(f"Master: Starting new crawl job. Seeds: {job_seed_urls}, Depth: {job_max_depth}. Will reply to rank {job_reply_to_rank} with tag {job_reply_tag}.")

            if not job_seed_urls:
                logging.warning("Master: Crawl job request received with no seed URLs. Skipping job and notifying server.")
                comm.send([], dest=job_reply_to_rank, tag=job_reply_tag) # Send empty list for no seeds
                continue # Go back to waiting for the next job request

            # Initialize per-job data structures
            urls_to_crawl_queue = list(job_seed_urls) # Queue for the current job
            crawled_urls_set = set(job_seed_urls)     # Set of all unique URLs found in this job
            
            job_task_count = 0           # Pub/Sub task counter for this job
            job_crawler_tasks_assigned = 0 # Number of tasks currently assigned to Pub/Sub for this job

            logging.info(f"Master: Job processing started. Initial queue size: {len(urls_to_crawl_queue)}. Assigned tasks: {job_crawler_tasks_assigned}.")

            # Inner loop for processing the current crawl job
            while urls_to_crawl_queue or job_crawler_tasks_assigned > 0:
                # Check for completed crawler tasks (Pub/Sub results via MPI from crawlers)
                # IMPORTANT: This assumes crawlers are MPI processes that send results back to master using MPI.
                # Tags used here (1, 99, 999, 98, 2) must be distinct from server communication tags if server is also a crawler.
                # If server is rank 1 and also a crawler, need careful tag/source checking here.
                # Assuming server (rank 1) messages are fully handled by handle_server_requests.
                if job_crawler_tasks_assigned > 0:
                    # Check for messages from ANY source (could be crawlers)
                    if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                        message_source = status.Get_source()
                        message_tag = status.Get_tag()

                        # Avoid re-processing a server message if it slipped through
                        if message_source == 1 and message_tag in [TAG_START_CRAWLING, TAG_SEARCH, TAG_NODES_STATUS]:
                            logging.debug(f"Master: Server message (source {message_source}, tag {message_tag}) detected during job. Will be handled in next outer loop.")
                        else:
                            # Process message (assumed from a crawler)
                            message_data = comm.recv(source=message_source, tag=message_tag)
                            if message_tag == 1: # Crawler task completed, sent back extracted URLs
                                job_crawler_tasks_assigned -= 1
                                newly_discovered_urls = message_data.get('urls', [])
                                if newly_discovered_urls:
                                    for url in newly_discovered_urls:
                                        crawled_urls_set.add(url) # Add to this job's set of found URLs
                                logging.info(f"Master Job: Crawler {message_source} sent {len(newly_discovered_urls)} URLs. Job's total unique: {len(crawled_urls_set)}. Job tasks assigned: {job_crawler_tasks_assigned}")
                            
                            elif message_tag == 99: # Crawler node reports status/heartbeat
                                logging.info(f"Master Job: Crawler {message_source} status: {message_data}")
                            elif message_tag == 999: # Crawler node reports error
                                logging.error(f"Master Job: Crawler {message_source} reported error: {message_data}")
                                #job_crawler_tasks_assigned -= 1 # Decrement task count
                            elif message_tag == 98:  # Heartbeat from a crawler
                                node_ip = message_data.get("ip_address", "N/A")
                                node_rank = message_data.get("rank", message_source) # Use message_source as fallback for rank
                                reported_status = message_data.get("status", "UNKNOWN")
                                node_info_map[node_rank] = {
                                    "type": "crawler",
                                    "ip": node_ip,
                                    "last_seen": time.time(),
                                    "reported_status": reported_status,
                                    "active": True # Mark active on heartbeat
                                }
                                logging.info(f"Master: Heartbeat/Status from CRAWLER Rank {node_rank} (IP: {node_ip}). Status: {reported_status}. Node map updated.")
                            elif message_tag == TAG_INDEXER_HEARTBEAT: # Added: Handle Indexer Heartbeat
                                node_ip = message_data.get("ip_address", "N/A")
                                node_rank = message_data.get("rank", message_source) # Use message_source as fallback for rank
                                node_info_map[node_rank] = {
                                    "type": "indexer",
                                    "ip": node_ip,
                                    "last_seen": time.time(),
                                    "reported_status": "ACTIVE", # Indexer heartbeat implies it's active
                                    "active": True # Mark active on heartbeat
                                }
                                logging.info(f"Master: Heartbeat from INDEXER Rank {node_rank} (IP: {node_ip}). Node map updated.")
                            elif message_tag == 2:  # Page content from crawler
                                logging.info(f"Master Job: Page content received from Crawler {message_source}. Forwarding to an indexer...")
                                if active_indexer_node_ranks:
                                    # Simple load balancing: send to the first available indexer or round-robin
                                    indexer_rank_to_send = active_indexer_node_ranks[job_task_count % len(active_indexer_node_ranks)]
                                    comm.send(message_data, dest=indexer_rank_to_send, tag=2) # Forward to specific indexer
                                else:
                                    logging.warning("Master Job: No active indexer nodes configured to forward page content.")
                            else:
                                logging.warning(f"Master Job: Received unhandled message tag {message_tag} from source {message_source}")
                
                # Assign new crawling tasks for the current job via Pub/Sub
                # Limit tasks assigned to be related to crawler_nodes_count (e.g., not to overwhelm Pub/Sub or crawlers)
                while urls_to_crawl_queue and job_crawler_tasks_assigned < crawler_nodes_count * 2: # Example: allow up to 2 tasks per nominal crawler
                    url_to_crawl = urls_to_crawl_queue.pop(0)
                    current_task_id_in_job = job_task_count
                    job_task_count += 1
                    
                    task_metadata = {"urls": [url_to_crawl], "max_depth": job_max_depth} # Use current job's max_depth
                    
                    message_json = json.dumps(task_metadata)
                    message_bytes = message_json.encode("utf-8")
                    try:
                        future = publisher.publish(topic_path, message_bytes)
                        # future.result() # Ensure publishing is complete, can be blocking
                        logging.info(f"Master Job: Published task {current_task_id_in_job} (crawl {url_to_crawl}, depth {job_max_depth}) to Pub/Sub. Tasks assigned for job: {job_crawler_tasks_assigned + 1}. Message ID: {future.result(timeout=10)}")
                        job_crawler_tasks_assigned += 1
                    except Exception as e:
                        logging.error(f"Master Job: Failed to publish task {current_task_id_in_job} to Pub/Sub: {e}. Re-adding '{url_to_crawl}' to queue.")
                        urls_to_crawl_queue.insert(0, url_to_crawl) # Re-add to front of queue
                        # Potentially break from this assignment loop if Pub/Sub is failing repeatedly
                        break 
                    
                    time.sleep(0.05) # Small delay between Pub/Sub posts
                
                time.sleep(0.2) # Main job processing loop sleep, check for crawler results/assign tasks

            # Current crawl job's inner loop has finished
            logging.info(f"Master: Crawl job for seeds {job_seed_urls} (depth {job_max_depth}) has completed processing.")
            logging.info(f"Master: Total unique URLs found for this job: {len(crawled_urls_set)}.")
            
            final_urls_list_for_job = list(crawled_urls_set)
            comm.send(final_urls_list_for_job, dest=job_reply_to_rank, tag=job_reply_tag)
            logging.info(f"Master: Sent final list of {len(final_urls_list_for_job)} URLs to server (rank {job_reply_to_rank}) for the completed job.")
            logging.info("Master: Ready for next crawl job request from server.")
            
        else:
            # No new crawl job was initiated by handle_server_requests in this iteration
            # (it might have handled an immediate request like status, or no request came)
            time.sleep(0.1) # Pause briefly before checking server for requests again

    # The following shutdown logic is now reachable if the loop is broken.
    logging.info("Master node main loop exited. Performing shutdown...")

    # Send shutdown signal to crawler nodes (if they are MPI processes expecting this)
    for crawler_rank_val in active_crawler_node_ranks:
        logging.info(f"Master: Sending shutdown signal (None, tag 0) to presumed Crawler MPI rank {crawler_rank_val}")
        comm.send(None, dest=crawler_rank_val, tag=0) # Empty task signals shutdown

    # Send shutdown to indexers too if they expect it
    for indexer_rank_val in active_indexer_node_ranks:
        logging.info(f"Master: Sending shutdown signal (None, tag 0) to presumed Indexer MPI rank {indexer_rank_val}")
        comm.send(None, dest=indexer_rank_val, tag=0) # Assuming indexers also shutdown on tag 0

    logging.info("Master Node Finished sending shutdown signals.")


if __name__ == '__main__':
    master_process()