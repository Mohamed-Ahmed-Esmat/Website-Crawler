from mpi4py import MPI
import logging
import socket
import os
<<<<<<< HEAD
from Indexer_States import IndexerStates
from utils import load_checkpoint, delete_checkpoint

=======
import subprocess
import pysolr
from google.cloud import storage
from Indexer_States import IndexerStates
from utils import load_checkpoint, delete_checkpoint

SOLR_URL = "http://10.10.0.43:8983/solr/"


>>>>>>> 4b8f7431785600f47fce6cb94a4abe4dae6bbcf9
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

<<<<<<< HEAD
=======
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

>>>>>>> 4b8f7431785600f47fce6cb94a4abe4dae6bbcf9
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

    while True:
        if state == "IDLE":
            state, data = IndexerStates.idle_state(comm)
        elif state == "Receiving_Data":
            state, data = IndexerStates.receiving_data_state(data, progress_point, rank)
        elif state == "Parsing":
            state, data = IndexerStates.parsing_state(data, progress_point, rank)
        elif state == "Indexing":
            state, data = IndexerStates.indexing_state(data, progress_point, rank)
        elif state == "Ready_For_Querying":
            state, data = IndexerStates.ready_for_querying_state(comm)
        elif state == "EXIT":
            break
        progress_point = None

<<<<<<< HEAD
if __name__ == "__main__":
=======
if __name__ == "_main_":
>>>>>>> 4b8f7431785600f47fce6cb94a4abe4dae6bbcf9
    indexer_node()