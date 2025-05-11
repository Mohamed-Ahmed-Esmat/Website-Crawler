from mpi4py import MPI
import logging
import socket
import json
import time
from pymongo import MongoClient
import os
import subprocess
import queue
import threading
from google.cloud import storage, pubsub_v1
from Indexer_States import IndexerStates
from concurrent.futures import ThreadPoolExecutor

hostname_indexer = socket.gethostname()
try:
    ip_address_indexer = socket.gethostbyname(hostname_indexer)
except socket.gaierror:
    ip_address_indexer = "unknown-ip-indexer"


SOLR_URL = "http://10.10.0.43:8983/solr/"

PROJECT_ID = "spheric-arcadia-457314-c8"
SUBSCRIPTION_NAME = "indexer-sub"
CONTENT_TOPIC_NAME = "crawler-indexer"

comm = MPI.COMM_WORLD
size = comm.Get_size()

hostname = socket.gethostname()
try:
    ip_address = socket.gethostbyname(hostname)
except:
    ip_address = "unknown-ip"

logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - {ip_address_indexer} - Indexer - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"indexer_{ip_address_indexer}.log"),
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

# Global executor for Pub/Sub message handling tasks
# Max workers can be tuned. Too many might thrash DB/Solr if they are bottlenecks.
# Too few might not utilize CPU if tasks are I/O bound and release GIL.
pubsub_task_executor = ThreadPoolExecutor(max_workers=os.cpu_count() or 1) # Default to CPU count or 1

def handle_pubsub_message(msg, FSM_state_machine_runner_comm_ref, FSM_state_machine_runner_executor_ref):
    """Callback for Pub/Sub messages. Offloads processing to the FSM runner if needed or directly to states."""
    try:
        msg.ack() # Acknowledge message immediately as we are about to process it (or offload)
        logging.info("Indexer: Received new crawled content from Pub/Sub.")
        content_data = json.loads(msg.data.decode("utf-8"))

        # The main FSM loop will pick this up if we signal it through an MPI message to self,
        # or if we directly invoke the state transition here if thread-safe.
        # For simplicity, if the FSM is primarily driven by MPI checks in idle_state,
        # we can directly transition here, assuming states are re-entrant or handle data well.
        # However, the FSM in indexer_node runs a loop. We need to feed data to it.
        # One way: Send an MPI message to itself with the data.
        # This allows the main FSM loop in indexer_node to pick it up via its comm.iprobe(tag=2).
        
        # Let's assume `comm` (FSM_state_machine_runner_comm_ref) is the MPI.COMM_WORLD object
        # and the indexer's own rank can receive messages sent to itself.
        rank_self = FSM_state_machine_runner_comm_ref.Get_rank()
        FSM_state_machine_runner_comm_ref.send(content_data, dest=rank_self, tag=2) # Send to self on tag 2
        logging.info(f"Pub/Sub Handler: Sent content for {content_data.get('url')} to own FSM via MPI tag 2.")

    except Exception as e:
        logging.error(f"⚠️ Error in Pub/Sub handle_pubsub_message: {e}", exc_info=True)
        # msg.nack() # Optional: Nack if processing truly failed and want Pub/Sub to redeliver

def indexer_node():
    global comm # Ensure comm is the global MPI.COMM_WORLD
    node_rank = comm.Get_rank()
    node_size = comm.Get_size()
    logging.info(f"Indexer node (Rank {node_rank}/{node_size}) started. IP: {ip_address_indexer}")

    # ThreadPoolExecutor for MPI tasks (like search) and for FSM background tasks
    # This single executor can be used by IndexerStates methods.
    # Max workers for MPI tasks + FSM tasks. Let's use a slightly larger pool if they are distinct.
    # If IndexerStates methods create their own threads, this might not be needed here for them.
    # Based on previous Indexer_States changes, it expects an executor to be passed.
    shared_executor = ThreadPoolExecutor(max_workers= (os.cpu_count() or 1) + 2) # e.g., CPU cores + 2 for MPI tasks

    # Initial heartbeat to master
    heartbeat_data = {
        "node_type": "indexer",
        "rank": node_rank,
        "ip_address": ip_address_indexer,
        "timestamp": time.time()
    }
    try:
        comm.send(heartbeat_data, dest=0, tag=97) # TAG_INDEXER_HEARTBEAT = 97
        logging.info(f"Sent initial heartbeat to Master: {heartbeat_data}")
    except MPI.Exception as e:
        logging.error(f"Failed to send initial heartbeat: {e}")

    # --- Pub/Sub Setup --- #
    subscriber = None
    streaming_pull_future = None
    try:
        subscriber = pubsub_v1.SubscriberClient()
        topic_path = subscriber.topic_path(PROJECT_ID, CONTENT_TOPIC_NAME) # crawler-indexer topic
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME) # indexer-sub
        
        try:
            subscriber.delete_subscription(subscription=subscription_path)
            logging.info(f"Deleted old Pub/Sub subscription: {SUBSCRIPTION_NAME}")
        except Exception: # Catch more specific exceptions if possible, e.g., NotFound
            logging.debug(f"Failed to delete Pub/Sub subscription {SUBSCRIPTION_NAME} (might not exist or other error).")
        
        subscriber.create_subscription(name=subscription_path, topic=topic_path)
        logging.info(f"Created new Pub/Sub subscription: {SUBSCRIPTION_NAME} to topic {CONTENT_TOPIC_NAME}")
            
       # flow_control = pubsub_v1.types.FlowControl(max_messages=5) # Allow a few messages in buffer
        
        # Curry comm and shared_executor into the callback
        # The Pub/Sub callback will run in a thread managed by the subscriber client.
        # It needs `comm` to send data to the main FSM loop via MPI self-send.
        # It doesn't directly use the `shared_executor` itself for its immediate task (which is MPI self-send).
        # The FSM states, when processing that MPI message, will use `shared_executor`.
        wrapped_callback = lambda msg: handle_pubsub_message(msg, comm, shared_executor)

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=wrapped_callback)
        logging.info(f"📥 Indexer subscribed to Pub/Sub topic: {CONTENT_TOPIC_NAME} via subscription {SUBSCRIPTION_NAME}")
        
    except Exception as e:
        logging.error(f"❌ Failed during Pub/Sub subscription setup: {e}", exc_info=True)
        # Fall through to FSM loop even if Pub/Sub fails, it can still do MPI tasks like search.
    # --- End Pub/Sub Setup --- #

    # --- Finite State Machine (FSM) Loop --- #
    current_FSM_state = "IDLE"
    data_for_FSM_state = None
    running = True

    logging.info("Starting Indexer FSM loop...")
    while running:
        try:
            if current_FSM_state == "IDLE":
                current_FSM_state, data_for_FSM_state = IndexerStates.idle_state(comm, shared_executor)
            elif current_FSM_state == "Receiving_Data":
                # No executor needed for receiving_data_state as it's quick validation
                current_FSM_state, data_for_FSM_state = IndexerStates.receiving_data_state(data_for_FSM_state)
            elif current_FSM_state == "Parsing":
                # No executor needed for parsing_state
                current_FSM_state, data_for_FSM_state = IndexerStates.parsing_state(data_for_FSM_state)
            elif current_FSM_state == "Indexing":
                current_FSM_state, data_for_FSM_state = IndexerStates.indexing_state(data_for_FSM_state, shared_executor)
            elif current_FSM_state == "Ready_For_Querying": # This state is mostly a passthrough now
                current_FSM_state, data_for_FSM_state = IndexerStates.ready_for_querying_state(comm, shared_executor)
            elif current_FSM_state == "Recovery":
                current_FSM_state, data_for_FSM_state = IndexerStates.recovery_state(data_for_FSM_state)
            elif current_FSM_state == "EXIT":
                logging.info("EXIT state reached. Shutting down indexer FSM loop.")
                running = False
                break
            else:
                logging.error(f"Unknown FSM state: {current_FSM_state}. Resetting to IDLE.")
                current_FSM_state = "IDLE"
                data_for_FSM_state = None
                time.sleep(1) # Pause if in unknown state
            
            # Small delay to prevent tight loop if all states return immediately
            # time.sleep(0.01) # IndexerStates.idle_state already has a sleep

        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt received in FSM loop. Shutting down...")
            running = False
            break
        except Exception as e:
            logging.error(f"Critical error in Indexer FSM loop: {e}", exc_info=True)
            current_FSM_state = "Recovery" # Attempt to recover
            data_for_FSM_state = {"original_state": current_FSM_state, "error": str(e)} # Pass error info to recovery
            time.sleep(1) # Pause before retrying FSM from recovery
    # --- End FSM Loop --- #

    logging.info("Indexer FSM loop has exited.")

    if streaming_pull_future:
        logging.info("Cancelling Pub/Sub streaming pull...")
        streaming_pull_future.cancel() # Signal the subscriber to stop
        try:
            streaming_pull_future.result(timeout=30) # Wait for cancellation to complete
            logging.info("Pub/Sub streaming pull cancelled.")
        except TimeoutError:
            logging.warning("Timeout waiting for Pub/Sub pull to cancel.")
        except Exception as e:
            logging.error(f"Error during Pub/Sub future cancellation: {e}")

    if subscriber:
        logging.info("Closing Pub/Sub subscriber client...")
        subscriber.close()
        logging.info("Pub/Sub subscriber client closed.")

    logging.info(f"Shutting down shared ThreadPoolExecutor...")
    shared_executor.shutdown(wait=True)
    logging.info("Shared ThreadPoolExecutor shut down.")
    
    # Restore MongoDB backups (moved to start as per original code, but could also be a shutdown task if needed)
    # restore_from_gcs("bucket-dist", "mongobackup/search_database")
    # restore_from_gcs("bucket-dist", "mongobackup/indexer")

    logging.info(f"Indexer node (Rank {node_rank}) process finished.")

if __name__ == "__main__":
    # Restore GCS is typically done once at startup, not repeatedly if main is called multiple times.
    # Assuming main() from main.py calls this indexer_node() once.
    if MPI.COMM_WORLD.Get_rank() != 0 and MPI.COMM_WORLD.Get_rank() != 1: # Simple check if it's an indexer candidate based on main.py logic
        restore_from_gcs("bucket-dist", "mongobackup/search_database")
        restore_from_gcs("bucket-dist", "mongobackup/indexer")
    
    try:
        indexer_node()
    except KeyboardInterrupt:
        logging.info("Indexer node shutting down due to KeyboardInterrupt in __main__.")
    except Exception as e:
        logging.error(f"Unhandled exception in indexer_node __main__: {e}", exc_info=True)
    finally:
        logging.info("Indexer __main__ finally block reached.")
        # MPI.Finalize() is handled by the main.py entry point