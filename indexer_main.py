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
from google.cloud import storage
from Indexer_States import IndexerStates
from utils import load_checkpoint, delete_checkpoint
from google.cloud import pubsub_v1


SOLR_URL = "http://10.10.0.43:8983/solr/"

# Flow control: single queue, single worker thread
message_queue = queue.Queue()
flow_lock = threading.Lock()

PROJECT_ID = "spheric-arcadia-457314-c8"
SUBSCRIPTION_NAME = "indexer-sub"


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
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

            # Feed the state machine manually
            state = "Receiving_Data"
            data = content_data
            progress_point = None

            while True:
                logging.info(f"🌀 Transitioning to state: {state}")
                if state == "IDLE":
                    state, data = IndexerStates.idle_state(comm)
                elif state == "Receiving_Data":
                    state, data = IndexerStates.receiving_data_state(data, progress_point)
                elif state == "Parsing":
                    state, data = IndexerStates.parsing_state(data, progress_point)
                elif state == "Indexing":
                    state, data = IndexerStates.indexing_state(data, progress_point)
                elif state == "Ready_For_Querying":
                    state, data = IndexerStates.ready_for_querying_state(comm)
                elif state == "EXIT":
                    break
                progress_point = None
        except Exception as e:
            logging.error(f"⚠️ Error in indexing flow: {e}")

def indexer_node():
    logging.info(f"Indexer node started with rank {rank} of {size}")

    checkpoint = load_checkpoint(rank)
    if checkpoint:
        state = checkpoint["state_name"]
        data = checkpoint["data"]
        progress_point = checkpoint.get("progress_point")
        logging.info(f"[Recovery] Node {rank} resumes from {state} with progress {progress_point}")
    else:
        delete_checkpoint(rank)
        state = "IDLE"
        data = None
        progress_point = None


    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)

    subscriber.subscribe(subscription_path, callback=handle_message, flow_control=flow_control)
    logging.info(f"📥 Indexer subscribed to topic: {SUBSCRIPTION_NAME}")


# Keep MPI listener alive to respond to master
    while True:
        if comm.iprobe(source=MPI.ANY_SOURCE, tag=2):
            request = comm.recv(source=MPI.ANY_SOURCE, tag=2)
            url = request.get("url")
            client = MongoClient("mongodb://localhost:27017/")
            page = client["search_database"]["indexed_pages"].find_one({"url": url})
            if page:
                comm.send(page, dest=0, tag=2)
                logging.info(f"📦 Sent indexed content of {url} to master via MPI")
            else:
                comm.send({"error": "not_found"}, dest=0, tag=2)
                logging.warning(f"❌ Requested content not found in DB: {url}")
        time.sleep(0.2)

if __name__ == "__main__":
    indexer_node()