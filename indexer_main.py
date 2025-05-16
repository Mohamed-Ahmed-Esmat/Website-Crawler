from mpi4py import MPI
import logging
import socket
import json
import time
from pymongo import MongoClient
import os
import subprocess
import pysolr
import queue
import threading
from google.cloud import storage, pubsub_v1
from Indexer_States import IndexerStates


SOLR_URL = "http://10.10.0.43:8983/solr/"

# Flow control: single queue, single worker thread
message_queue = queue.Queue()
flow_lock = threading.Lock()

PROJECT_ID = "spheric-arcadia-457314-c8"
SUBSCRIPTION_NAME = "indexer-sub"

TAG_INDEXER_HEARTBEAT = 97 
TAG_STATUS_UPDATE = 98


comm = MPI.COMM_WORLD
size = comm.Get_size()

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

def restore_from_gcs(bucket_name, gcs_file_path):
    """
    Downloads a BSON file from GCS and restores it into MongoDB.
    """
    logging.info(f"🧩 Restoring MongoDB from GCS file: gs://{bucket_name}/{gcs_file_path}")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)

        local_file = f"/tmp/{os.path.basename(gcs_file_path)}"
        blob.download_to_filename(local_file)

        # Infer db name
        if "search_database" in gcs_file_path:
            db_name = "search_database"
        elif "indexer" in gcs_file_path:
            db_name = "indexer"
        else:
            db_name = "default"

        subprocess.run(["mongorestore", "--db", db_name, "--drop", local_file], check=True)
        logging.info(f"✅ MongoDB database '{db_name}' restored successfully.")
    except Exception as e:
        logging.error(f"❌ Restore failed: {e}")

def handle_message(msg):
    msg.ack()
    with flow_lock:  # Flow control: wait till current task is done
        try:
            logging.info("✅ Indexer received new crawled content")
            content_data = json.loads(msg.data.decode("utf-8"))

            # Send progress update to master
            comm.send({
                "status": "WORKING",
                "current_task": content_data.get("url", "Unknown URL"),
                "progress": {
                    "current": 1,
                    "total": 1,
                    "percentage": 0
                }
            }, dest=0, tag=TAG_STATUS_UPDATE)

            # Feed the state machine manually
            state = "Receiving_Data"
            data = content_data
            progress_point = None

            while True:
                logging.info(f"🌀 Transitioning to state: {state}")
                
                # Update progress based on state
                progress_info = {
                    "status": "WORKING",
                    "current_task": data.get("url", "Unknown URL") if isinstance(data, dict) else "Processing",
                    "progress": {
                        "current": 1,
                        "total": 1,
                        "percentage": 0
                    }
                }
                
                if state == "IDLE":
                    state, data = IndexerStates.idle_state(comm)
                    progress_info["status"] = "IDLE"
                elif state == "Receiving_Data":
                    state, data = IndexerStates.receiving_data_state(data, progress_point)
                    progress_info["progress"]["percentage"] = 25
                elif state == "Parsing":
                    state, data = IndexerStates.parsing_state(data, progress_point)
                    progress_info["progress"]["percentage"] = 50
                elif state == "Indexing":
                    state, data = IndexerStates.indexing_state(data, progress_point)
                    progress_info["progress"]["percentage"] = 75
                elif state == "EXIT":
                    progress_info["status"] = "IDLE"
                    progress_info["progress"]["percentage"] = 100
                    break
                
                # Send progress update
                comm.send(progress_info, dest=0, tag=TAG_STATUS_UPDATE)
                progress_point = None
                
        except Exception as e:
            logging.error(f"⚠️ Error in indexing flow: {e}")
            # Send error status
            comm.send({
                "status": "ERROR",
                "current_task": "Error occurred",
                "progress": {
                    "current": 0,
                    "total": 1,
                    "percentage": 0
                }
            }, dest=0, tag=TAG_STATUS_UPDATE)

def indexer_node():
    logging.info(f"Indexer node started with size of {size}")

    try:
        subscriber = pubsub_v1.SubscriberClient()
        topic_path = subscriber.topic_path(PROJECT_ID, "crawler-indexer")
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
        
        # Try to delete the old subscription if it exists
        try:
            subscriber.delete_subscription(subscription=subscription_path)
            logging.info(f"Deleted old subscription: {SUBSCRIPTION_NAME}")
        except Exception as e:
            logging.error(f"❌ Failed to delete subscription (might not exist): {e}")
        
        # Create a new subscription to the topic
        try:
            subscriber.create_subscription(
                name=subscription_path, 
                topic=topic_path
            )
            logging.info(f"✅ Created new subscription: {SUBSCRIPTION_NAME}")
        except Exception as e:
            logging.error(f"❌ Failed to create subscription: {e}")
            
    except Exception as e:
        logging.error(f"❌ Failed during subscription setup: {e}")
    
    # Create a new subscriber client for the subscription
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)

    # Subscribe to the topic
    try:
        streaming_pull_future = subscriber.subscribe(
            subscription_path, 
            callback=handle_message, 
            flow_control=flow_control
        )
        logging.info(f"📥 Indexer subscribed to topic: {SUBSCRIPTION_NAME}")
        
        last_heartbeat = 0
        # Keep MPI listener alive to respond to master and also keep the subscription active
        while True:
            current_time = time.time()
            if current_time - last_heartbeat >= 5:                
                rank_indexer = comm.Get_rank() # Get rank within the method
                heartbeat_data = {
                    "node_type": "indexer",
                    "rank": rank_indexer,
                    "ip_address": ip_address,
                    "timestamp": time.time()
                }
                comm.send(heartbeat_data, dest=0, tag=TAG_INDEXER_HEARTBEAT)
                logging.info(f"Sent Heartbeat to Master: {heartbeat_data.get('node_type')} {heartbeat_data.get('rank')}")
                last_heartbeat = current_time
            if comm.iprobe(source=0, tag=20):
                search_request = comm.recv(source=0, tag=20)
                logging.info(f"🔍 Received search request from master: {search_request}")

                query = search_request.get("query", "")
                search_type = search_request.get("search_type", "keyword")

                if not query:
                    logging.warning("❌ Received empty search query from master")
                    comm.send([], dest=0, tag=21)  # TAG_INDEXER_SEARCH_RESULTS = 21
                else:
                    try:
                        # Use the IndexerStates.search method to perform the search
                        search_results = IndexerStates.search(query, search_type)

                        # Extract URLs from the Solr results
                        urls = []
                        if search_results:
                            for result in search_results:
                                url = result.get("id")
                                if url:
                                    urls.append(url)

                        logging.info(f"🔍 Search for '{query}' ({search_type}) found {len(urls)} results")

                        # Send results back to master
                        comm.send(urls, dest=0, tag=21)

                    except Exception as e:
                        logging.error(f"❌ Error processing search request: {e}")
                        comm.send([], dest=0, tag=21)  # Send empty results on error

            time.sleep(0.2)
            
    except Exception as e:
        logging.error(f"❌ Failed to subscribe to topic: {e}")
        # Make sure to close the subscriber client
        if 'streaming_pull_future' in locals():
            streaming_pull_future.cancel()
        subscriber.close()

if __name__ == "__main__":

    # Restore MongoDB backups
    restore_from_gcs("bucket-dist", "mongobackup/search_database")
    restore_from_gcs("bucket-dist", "mongobackup/indexer")

    
    indexer_node()