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

hostname_indexer = socket.gethostname()
try:
    ip_address_indexer = socket.gethostbyname(hostname_indexer)
except socket.gaierror:
    ip_address_indexer = "unknown-ip-indexer"


SOLR_URL = "http://10.10.0.43:8983/solr/"

# Flow control: single queue, single worker thread
message_queue = queue.Queue()
flow_lock = threading.Lock()

PROJECT_ID = "spheric-arcadia-457314-c8"
SUBSCRIPTION_NAME = "indexer-sub"


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
    """
    Callback for Pub/Sub messages - designed to return quickly
    """
    try:
        # Acknowledge message immediately
        msg.ack()
        logging.info("✅ Indexer received new message from Pub/Sub")
        
        # Parse the message data
        content_data = json.loads(msg.data.decode("utf-8"))
        
        # Use a new thread to process the message through the state machine
        # This ensures we don't block the Pub/Sub callback which could cause issues
        thread = threading.Thread(
            target=process_message_data,
            args=(content_data,)
        )
        thread.daemon = True  # Background thread won't block program exit
        thread.start()
        
    except Exception as e:
        logging.error(f"⚠️ Error in handle_message: {e}")

def process_message_data(content_data):
    """
    Process a message through the state machine with proper locking
    """
    # Acquire lock during processing to prevent concurrent state machine executions
    if not flow_lock.acquire(timeout=30):  # 30-second timeout
        logging.error("⚠️ Failed to acquire flow_lock after 30 seconds. Message processing skipped.")
        return
        
    try:
        logging.info("🔄 Starting state machine processing for new content")
        
        # Feed the state machine manually
        state = "Receiving_Data"
        data = content_data
        progress_point = None
        
        # Add a safety counter to prevent infinite loops
        state_transition_count = 0
        max_state_transitions = 20  # Reasonable maximum for a single message
        
        while state and state_transition_count < max_state_transitions:
            state_transition_count += 1
            logging.info(f"🌀 Transitioning to state: {state} (transition {state_transition_count}/{max_state_transitions})")
            
            # Skip Ready_For_Querying to avoid blocking on input
            if state == "Ready_For_Querying":
                logging.info("⏩ Skipping Ready_For_Querying state to avoid potential blocking. Going to IDLE.")
                state = "IDLE"
                continue
                
            try:
                # Process current state
                if state == "IDLE":
                    # Override the IDLE state for message processing flow
                    # This ensures we finish processing this message before listening for more
                    break  # Exit the while loop to complete this message processing
                elif state == "Receiving_Data":
                    state, data = IndexerStates.receiving_data_state(data, progress_point)
                elif state == "Parsing":
                    state, data = IndexerStates.parsing_state(data, progress_point)
                elif state == "Indexing":
                    state, data = IndexerStates.indexing_state(data, progress_point)
                elif state == "Recovery":
                    # Handle recovery more gracefully
                    logging.info("🔄 Recovery state reached - returning to IDLE")
                    state = "IDLE"
                    break  # Exit to avoid potential recovery loops
                elif state == "EXIT":
                    break
                    
                # Check for null state
                if state is None:
                    logging.error("⚠️ State machine returned None state. Exiting processing loop.")
                    break
                    
                progress_point = None
                
            except Exception as e:
                logging.error(f"⚠️ Error in state {state}: {e}", exc_info=True)
                # On error, switch to IDLE and exit the loop
                break
                
        # Log completion of message processing
        if state_transition_count >= max_state_transitions:
            logging.warning(f"⚠️ Hit maximum state transitions ({max_state_transitions}). Potential infinite loop detected and broken.")
        else:
            logging.info(f"✅ Message processing completed successfully in {state_transition_count} state transitions")
            
    except Exception as e:
        logging.error(f"⚠️ Global error in message processing: {e}", exc_info=True)
    finally:
        # Always release the lock
        flow_lock.release()
        logging.debug("🔓 Released flow lock after message processing")

def indexer_node():
    logging.info(f"Indexer node started with size of {size}")

    # Send initial heartbeat
    rank_indexer = comm.Get_rank()
    heartbeat_data = {
        "node_type": "indexer",
        "rank": rank_indexer,
        "ip_address": ip_address_indexer,
        "timestamp": time.time()
    }
    comm.send(heartbeat_data, dest=0, tag=97)
    logging.info("✅ Sent initial heartbeat to master")
    
    last_heartbeat_time = time.time()
    last_subscription_check = time.time()
    subscription_check_interval = 60  # Check every 60 seconds
    heartbeat_interval = 10  # Send heartbeat every 10 seconds

    try:
        # Set up Pub/Sub client
        subscriber = pubsub_v1.SubscriberClient()
        topic_path = subscriber.topic_path(PROJECT_ID, "crawler-indexer")
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
        
        # Delete and recreate subscription for clean start
        try:
            subscriber.delete_subscription(subscription=subscription_path)
            logging.info(f"🗑️ Deleted old subscription: {SUBSCRIPTION_NAME}")
        except Exception as e:
            logging.debug(f"⚠️ Failed to delete subscription (might not exist): {e}")
        
        time.sleep(1)  # Brief pause to ensure deletion is complete
        
        try:
            subscriber.create_subscription(
                name=subscription_path, 
                topic=topic_path
            )
            logging.info(f"✅ Created new subscription: {SUBSCRIPTION_NAME}")
        except Exception as e:
            logging.error(f"❌ Failed to create subscription: {e}")
            
        # Set up flow control to limit concurrent processing
        flow_control = pubsub_v1.types.FlowControl(max_messages=2)
        
        # Subscribe to the topic
        streaming_pull_future = subscriber.subscribe(
            subscription_path, 
            callback=handle_message, 
            flow_control=flow_control
        )
        logging.info(f"📥 Indexer subscribed to topic: {SUBSCRIPTION_NAME}")
        
        # Keep MPI listener alive and maintain Pub/Sub subscription
        while True:
            current_time = time.time()
            
            # Send periodic heartbeats
            if current_time - last_heartbeat_time > heartbeat_interval:
                heartbeat_data = {
                    "node_type": "indexer",
                    "rank": rank_indexer,
                    "ip_address": ip_address_indexer,
                    "timestamp": current_time
                }
                comm.send(heartbeat_data, dest=0, tag=97)
                last_heartbeat_time = current_time
                logging.debug("📈 Sent heartbeat to master")
            
            # Check for MPI messages
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=MPI.Status()):
                tag = MPI.Status().Get_tag()
                source = MPI.Status().Get_source()
                
                if tag == 2:  # Content request
                    request = comm.recv(source=source, tag=2)
                    url = request.get("url")
                    if url:
                        client = MongoClient("mongodb://localhost:27017/")
                        page = client["search_database"]["indexed_pages"].find_one({"url": url})
                        if page:
                            comm.send(page, dest=source, tag=2)
                            logging.info(f"📦 Sent indexed content of {url} to rank {source} via MPI")
                        else:
                            comm.send({"error": "not_found"}, dest=source, tag=2)
                            logging.warning(f"❌ Requested content not found in DB: {url}")
                elif tag == 20:  # Search query from master
                    # Let IndexerStates.idle_state handle this - just log it
                    logging.info(f"👁️ Search query received from rank {source}")
                    # The next idle_state iteration will process this
                elif tag == 0:  # Shutdown signal
                    logging.info(f"⚠️ Shutdown signal received from rank {source}. Exiting...")
                    break
                else:
                    # Receive unknown message to clear the buffer
                    unknown_msg = comm.recv(source=source, tag=tag)
                    logging.warning(f"❓ Received unknown message with tag {tag} from rank {source}")
            
            # Periodically check if the subscription is still running
            if current_time - last_subscription_check > subscription_check_interval:
                last_subscription_check = current_time
                
                if hasattr(streaming_pull_future, 'done') and streaming_pull_future.done():
                    logging.warning("⚠️ Pub/Sub subscription future has completed. Attempting to resubscribe...")
                    try:
                        # Cancel old future if possible
                        streaming_pull_future.cancel()
                        # Create new subscription
                        streaming_pull_future = subscriber.subscribe(
                            subscription_path, 
                            callback=handle_message, 
                            flow_control=flow_control
                        )
                        logging.info("✅ Successfully resubscribed to Pub/Sub topic")
                    except Exception as e:
                        logging.error(f"❌ Failed to resubscribe to Pub/Sub: {e}")
                else:
                    logging.debug("✅ Pub/Sub subscription is still active")
            
            # Brief sleep to prevent tight CPU usage
            time.sleep(0.1)
            
    except Exception as e:
        logging.error(f"❌ Critical error in indexer node: {e}", exc_info=True)
    finally:
        # Cleanup
        try:
            logging.info("🧹 Performing cleanup...")
            if 'streaming_pull_future' in locals():
                streaming_pull_future.cancel()
                logging.info("✅ Cancelled Pub/Sub streaming future")
            if 'subscriber' in locals():
                subscriber.close()
                logging.info("✅ Closed Pub/Sub subscriber client")
        except Exception as cleanup_error:
            logging.error(f"❌ Error during cleanup: {cleanup_error}")
        
        logging.info("👋 Indexer node shutting down")

if __name__ == "__main__":

    # Restore MongoDB backups
    restore_from_gcs("bucket-dist", "mongobackup/search_database")
    restore_from_gcs("bucket-dist", "mongobackup/indexer")

    
    indexer_node()